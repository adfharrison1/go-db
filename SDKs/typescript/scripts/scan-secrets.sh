#!/bin/bash

# Secrets scanning script for npm publish
# This script scans the codebase for potential secrets before publishing

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if we're in the right directory
if [ ! -f "package.json" ]; then
    print_error "package.json not found. Please run this script from the SDKs/typescript directory."
    exit 1
fi

print_status "Starting secrets scan..."

# Create temporary directory for scan results
TEMP_DIR=$(mktemp -d)
SCAN_RESULTS="$TEMP_DIR/scan-results.txt"

# Function to check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to scan with basic patterns (no external tools)
basic_secrets_scan() {
    print_status "Running basic secrets scan..."
    
    # High-confidence secret patterns (more specific)
    local high_confidence_patterns=(
        "sk-[a-zA-Z0-9]{20,}"  # OpenAI API keys
        "pk_[a-zA-Z0-9]{20,}"  # Stripe keys
        "sk_[a-zA-Z0-9]{20,}"  # Stripe keys
        "AKIA[0-9A-Z]{16}"     # AWS Access Key ID
        "AIza[0-9A-Za-z\\-_]{35}"  # Google API keys
        "ya29\\.[0-9A-Za-z\\-_]+"  # Google OAuth tokens
        "1//[0-9A-Za-z\\-_]+"  # Google OAuth tokens
        "AIza[0-9A-Za-z\\-_]{35}"  # Google API keys
        "xoxb-[0-9]{11}-[0-9]{11}-[0-9A-Za-z]{24}"  # Slack bot tokens
        "xoxp-[0-9]{11}-[0-9]{11}-[0-9A-Za-z]{24}"  # Slack user tokens
        "xoxa-[0-9]{11}-[0-9]{11}-[0-9A-Za-z]{24}"  # Slack app tokens
        "xoxr-[0-9]{11}-[0-9]{11}-[0-9A-Za-z]{24}"  # Slack refresh tokens
        "ghp_[a-zA-Z0-9]{36}"  # GitHub personal access tokens
        "gho_[a-zA-Z0-9]{36}"  # GitHub OAuth tokens
        "ghu_[a-zA-Z0-9]{36}"  # GitHub user tokens
        "ghs_[a-zA-Z0-9]{36}"  # GitHub server tokens
        "ghr_[a-zA-Z0-9]{36}"  # GitHub refresh tokens
        "eyJ[A-Za-z0-9+/]{20,}={0,2}"  # JWT tokens
        "-----BEGIN PRIVATE KEY-----"  # Private keys
        "-----BEGIN RSA PRIVATE KEY-----"  # RSA private keys
        "-----BEGIN OPENSSH PRIVATE KEY-----"  # SSH private keys
        "-----BEGIN DSA PRIVATE KEY-----"  # DSA private keys
        "-----BEGIN EC PRIVATE KEY-----"  # EC private keys
    )
    
    # Medium-confidence patterns (context-dependent)
    local medium_confidence_patterns=(
        "api[_-]?key\\s*[:=]\\s*['\"][^'\"]{20,}['\"]"  # API keys with assignment
        "secret[_-]?key\\s*[:=]\\s*['\"][^'\"]{20,}['\"]"  # Secret keys with assignment
        "private[_-]?key\\s*[:=]\\s*['\"][^'\"]{20,}['\"]"  # Private keys with assignment
        "access[_-]?token\\s*[:=]\\s*['\"][^'\"]{20,}['\"]"  # Access tokens with assignment
        "auth[_-]?token\\s*[:=]\\s*['\"][^'\"]{20,}['\"]"  # Auth tokens with assignment
        "password\\s*[:=]\\s*['\"][^'\"]{8,}['\"]"  # Passwords with assignment
        "aws[_-]?access[_-]?key[_-]?id\\s*[:=]\\s*['\"][^'\"]{20,}['\"]"  # AWS access key IDs
        "aws[_-]?secret[_-]?access[_-]?key\\s*[:=]\\s*['\"][^'\"]{20,}['\"]"  # AWS secret keys
        "github[_-]?token\\s*[:=]\\s*['\"][^'\"]{20,}['\"]"  # GitHub tokens
        "npm[_-]?token\\s*[:=]\\s*['\"][^'\"]{20,}['\"]"  # NPM tokens
        "docker[_-]?hub[_-]?token\\s*[:=]\\s*['\"][^'\"]{20,}['\"]"  # Docker Hub tokens
        "jwt[_-]?secret\\s*[:=]\\s*['\"][^'\"]{20,}['\"]"  # JWT secrets
        "session[_-]?secret\\s*[:=]\\s*['\"][^'\"]{20,}['\"]"  # Session secrets
        "encryption[_-]?key\\s*[:=]\\s*['\"][^'\"]{20,}['\"]"  # Encryption keys
        "database[_-]?password\\s*[:=]\\s*['\"][^'\"]{8,}['\"]"  # Database passwords
        "mongodb[_-]?uri\\s*[:=]\\s*['\"][^'\"]{20,}['\"]"  # MongoDB URIs
        "postgres[_-]?uri\\s*[:=]\\s*['\"][^'\"]{20,}['\"]"  # PostgreSQL URIs
        "mysql[_-]?uri\\s*[:=]\\s*['\"][^'\"]{20,}['\"]"  # MySQL URIs
        "connection[_-]?string\\s*[:=]\\s*['\"][^'\"]{20,}['\"]"  # Connection strings
    )
    
    local found_secrets=false
    
    # Scan for high-confidence patterns first
    print_status "Scanning for high-confidence secret patterns..."
    for pattern in "${high_confidence_patterns[@]}"; do
        if grep -r -n \
            --include="*.ts" --include="*.js" --include="*.json" --include="*.md" --include="*.txt" --include="*.yml" --include="*.yaml" --include="*.env*" --include="*.config*" \
            --exclude-dir="node_modules" --exclude-dir="dist" --exclude-dir="build" --exclude-dir="coverage" --exclude-dir=".git" \
            -E "$pattern" . > "$SCAN_RESULTS" 2>/dev/null; then
            print_warning "HIGH CONFIDENCE: Potential secret pattern found: $pattern"
            cat "$SCAN_RESULTS" | head -5
            found_secrets=true
        fi
    done
    
    # Scan for medium-confidence patterns
    print_status "Scanning for medium-confidence secret patterns..."
    for pattern in "${medium_confidence_patterns[@]}"; do
        if grep -r -n \
            --include="*.ts" --include="*.js" --include="*.json" --include="*.md" --include="*.txt" --include="*.yml" --include="*.yaml" --include="*.env*" --include="*.config*" \
            --exclude-dir="node_modules" --exclude-dir="dist" --exclude-dir="build" --exclude-dir="coverage" --exclude-dir=".git" \
            -E "$pattern" . > "$SCAN_RESULTS" 2>/dev/null; then
            # Filter out false positives
            if grep -v -E "(example|sample|test|demo|placeholder|TODO|FIXME|XXX|example\.com|localhost|127\.0\.0\.1|0\.0\.0\.0|node_modules|\.d\.ts|README|CHANGELOG|CONTRIBUTING|baseURL|url|port|username|user|login)" "$SCAN_RESULTS" > /dev/null; then
                print_warning "MEDIUM CONFIDENCE: Potential secret pattern found: $pattern"
                grep -v -E "(example|sample|test|demo|placeholder|TODO|FIXME|XXX|example\.com|localhost|127\.0\.0\.1|0\.0\.0\.0|node_modules|\.d\.ts|README|CHANGELOG|CONTRIBUTING|baseURL|url|port|username|user|login)" "$SCAN_RESULTS" | head -3
                found_secrets=true
            fi
        fi
    done
    
    if [ "$found_secrets" = true ]; then
        print_error "Potential secrets detected! Please review the findings above."
        print_status "If these are false positives, you can add them to .gitignore or update this script."
        return 1
    else
        print_success "No obvious secrets detected in basic scan."
        return 0
    fi
}

# Function to scan with truffleHog (if available)
trufflehog_scan() {
    if command_exists trufflehog; then
        print_status "Running truffleHog scan..."
        if trufflehog filesystem . --no-verification --format json > "$TEMP_DIR/trufflehog-results.json" 2>/dev/null; then
            local findings=$(jq length "$TEMP_DIR/trufflehog-results.json" 2>/dev/null || echo "0")
            if [ "$findings" -gt 0 ]; then
                print_error "truffleHog found $findings potential secrets!"
                jq -r '.[] | "\(.Detector) in \(.File): \(.Raw)"' "$TEMP_DIR/trufflehog-results.json" | head -10
                return 1
            else
                print_success "truffleHog scan passed."
                return 0
            fi
        else
            print_warning "truffleHog scan failed, continuing with basic scan..."
            return 0
        fi
    else
        print_status "truffleHog not installed, skipping..."
        return 0
    fi
}

# Function to scan with gitleaks (if available)
gitleaks_scan() {
    if command_exists gitleaks; then
        print_status "Running gitleaks scan..."
        if gitleaks detect --source . --report-format json --report-path "$TEMP_DIR/gitleaks-results.json" 2>/dev/null; then
            local findings=$(jq length "$TEMP_DIR/gitleaks-results.json" 2>/dev/null || echo "0")
            if [ "$findings" -gt 0 ]; then
                print_error "gitleaks found $findings potential secrets!"
                jq -r '.[] | "\(.RuleID) in \(.File): \(.Secret)"' "$TEMP_DIR/gitleaks-results.json" | head -10
                return 1
            else
                print_success "gitleaks scan passed."
                return 0
            fi
        else
            print_warning "gitleaks scan failed, continuing with basic scan..."
            return 0
        fi
    else
        print_status "gitleaks not installed, skipping..."
        return 0
    fi
}

# Function to check for common secret patterns in specific file types
check_specific_files() {
    print_status "Checking specific file types..."
    
    local issues_found=false
    
    # Check package.json for sensitive data (more specific patterns)
    if grep -q -E "(password\\s*[:=]|secret\\s*[:=]|key\\s*[:=]|token\\s*[:=]|credential\\s*[:=])" package.json 2>/dev/null; then
        print_warning "Potential sensitive data in package.json"
        grep -E "(password\\s*[:=]|secret\\s*[:=]|key\\s*[:=]|token\\s*[:=]|credential\\s*[:=])" package.json
        issues_found=true
    fi
    
    # Check for .env files
    if find . -name "*.env*" -not -path "./node_modules/*" -not -path "./dist/*" -not -path "./build/*" -not -path "./coverage/*" | grep -q .; then
        print_warning "Found .env files that might contain secrets"
        find . -name "*.env*" -not -path "./node_modules/*" -not -path "./dist/*" -not -path "./build/*" -not -path "./coverage/*"
        issues_found=true
    fi
    
    # Check for config files with potential secrets
    if find . -name "*.config.*" -not -path "./node_modules/*" -not -path "./dist/*" -not -path "./build/*" -not -path "./coverage/*" | xargs grep -l -E "(password|secret|key|token)" 2>/dev/null | grep -q .; then
        print_warning "Found config files with potential secrets"
        issues_found=true
    fi
    
    if [ "$issues_found" = true ]; then
        return 1
    else
        print_success "No issues found in specific file checks."
        return 0
    fi
}

# Main scanning logic
main() {
    local scan_passed=true
    
    # Run basic scan
    if ! basic_secrets_scan; then
        scan_passed=false
    fi
    
    # Run truffleHog scan
    if ! trufflehog_scan; then
        scan_passed=false
    fi
    
    # Run gitleaks scan
    if ! gitleaks_scan; then
        scan_passed=false
    fi
    
    # Check specific files
    if ! check_specific_files; then
        scan_passed=false
    fi
    
    # Cleanup
    rm -rf "$TEMP_DIR"
    
    if [ "$scan_passed" = true ]; then
        print_success "All secrets scans passed! Safe to publish."
        exit 0
    else
        print_error "Secrets scan failed! Please review and fix issues before publishing."
        print_status "To install additional scanning tools:"
        print_status "  brew install trufflehog gitleaks  # macOS"
        print_status "  pip install trufflehog            # Python"
        print_status "  go install github.com/zricethezav/gitleaks/v8@latest  # Go"
        exit 1
    fi
}

# Run the scan
main
