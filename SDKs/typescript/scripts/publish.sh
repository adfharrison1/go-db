#!/bin/bash

# Publishing script for @adfharrison1/go-db-typescript-sdk
# Usage: ./scripts/publish.sh [patch|minor|major|beta]

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

# Check if npm is logged in
if ! npm whoami > /dev/null 2>&1; then
    print_error "Not logged in to npm. Please run 'npm login' first."
    exit 1
fi

print_status "Starting publish process..."

# Step 1: Clean and install dependencies
print_status "Cleaning and installing dependencies..."
yarn clean
yarn install

# Step 2: Scan for secrets
print_status "Scanning for secrets..."
yarn scan:secrets

# Step 3: Run tests
print_status "Running tests..."
yarn test:ci

# Step 4: Build package
print_status "Building package..."
yarn build

# Step 4: Check what will be published
print_status "Checking what will be published..."
yarn check:publish

# Step 5: Determine version bump
VERSION_TYPE=${1:-patch}

case $VERSION_TYPE in
    patch|minor|major|beta)
        print_status "Publishing as $VERSION_TYPE..."
        ;;
    *)
        print_error "Invalid version type: $VERSION_TYPE"
        print_status "Usage: $0 [patch|minor|major|beta]"
        exit 1
        ;;
esac

# Step 6: Confirm before publishing
print_warning "About to publish @adfharrison1/go-db-typescript-sdk"
print_warning "Version type: $VERSION_TYPE"
read -p "Continue? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    print_status "Publishing cancelled."
    exit 0
fi

# Step 7: Publish
if [ "$VERSION_TYPE" = "beta" ]; then
    print_status "Publishing beta version..."
    yarn publish:beta
else
    print_status "Publishing $VERSION_TYPE version..."
    case $VERSION_TYPE in
        patch)
            yarn publish:patch
            ;;
        minor)
            yarn publish:minor
            ;;
        major)
            yarn publish:major
            ;;
    esac
fi

# Step 8: Verify publication
print_status "Verifying publication..."
sleep 2  # Give npm registry time to update

if npm view @adfharrison1/go-db-typescript-sdk version > /dev/null 2>&1; then
    VERSION=$(npm view @adfharrison1/go-db-typescript-sdk version)
    print_success "Successfully published @adfharrison1/go-db-typescript-sdk@$VERSION"
    
    if [ "$VERSION_TYPE" = "beta" ]; then
        print_status "Beta version published. Install with: npm install @adfharrison1/go-db-typescript-sdk@beta"
    else
        print_status "Latest version published. Install with: npm install @adfharrison1/go-db-typescript-sdk@latest"
    fi
else
    print_error "Failed to verify publication. Check npm registry manually."
    exit 1
fi

print_success "Publishing complete!"
