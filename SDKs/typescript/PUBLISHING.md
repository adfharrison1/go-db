# Publishing Guide for @adfharrison1/go-db-typescript-sdk

This guide covers how to publish the TypeScript SDK to npm using local commands.

## Prerequisites

1. **npm Account**: Make sure you're logged in to npm

   ```bash
   npm whoami  # Should show your username
   npm login   # If not logged in
   ```

2. **Package Access**: Ensure you have publish access to the `@go-db` scope
   ```bash
   npm access list packages @go-db
   ```

## Publishing Workflow

### 1. **Pre-Publish Checks**

Always run these commands before publishing:

```bash
# Check what will be published
yarn check:publish

# Run all tests
yarn test:ci

# Build the package
yarn build

# Dry run to see what would be published
yarn publish:dry-run
```

### 2. **Version Management**

Choose the appropriate version bump:

```bash
# Patch version (1.0.0 -> 1.0.1) - Bug fixes
yarn version:patch

# Minor version (1.0.0 -> 1.1.0) - New features, backward compatible
yarn version:minor

# Major version (1.0.0 -> 2.0.0) - Breaking changes
yarn version:major
```

### 3. **Publishing Options**

#### **Latest Release (Stable)**

```bash
# For patch releases
yarn publish:patch

# For minor releases
yarn publish:minor

# For major releases
yarn publish:major

# Or manually
yarn publish:latest
```

#### **Beta Release**

```bash
# Publish as beta (won't affect latest tag)
yarn publish:beta
```

### 4. **Verification**

After publishing, verify the package:

```bash
# Check if package is available
npm view @adfharrison1/go-db-typescript-sdk

# Test installation
npm install @adfharrison1/go-db-typescript-sdk@latest
```

## Publishing Checklist

- [ ] All tests pass (`yarn test:ci`)
- [ ] Build succeeds (`yarn build`)
- [ ] Version number is correct
- [ ] CHANGELOG.md is updated (if you have one)
- [ ] README.md is up to date
- [ ] No sensitive data in published files
- [ ] Dry run completed successfully

## Common Commands

```bash
# Check current version
npm view @adfharrison1/go-db-typescript-sdk version

# Check what files will be published
yarn check:publish

# Test the build
yarn build && ls -la dist/

# Run tests
yarn test

# Clean and rebuild
yarn clean && yarn build
```

## Troubleshooting

### **Permission Denied**

```bash
# Check if you're logged in
npm whoami

# Check package access
npm access list packages @go-db

# If needed, request access to the scope
npm access grant read-write @go-db:adfharrison1
```

### **Version Already Exists**

```bash
# Check current published version
npm view @adfharrison1/go-db-typescript-sdk version

# Bump version appropriately
yarn version:patch  # or minor/major
```

### **Build Errors**

```bash
# Clean everything and rebuild
yarn clean
rm -rf node_modules
yarn install
yarn build
```

## Package Information

- **Package Name**: `go-db-typescript-sdk`
- **Registry**: https://registry.npmjs.org/
- **Scope**: None (unscoped package)
- **Current Version**: Check `package.json`

## Files Included in Package

The following files are included in the published package:

- `dist/**/*` - Compiled JavaScript and TypeScript definitions
- `README.md` - Package documentation
- `package.json` - Package metadata
- `LICENSE` - License file (if present)

Files excluded:

- `src/**/*` - Source TypeScript files
- `__tests__/**/*` - Test files
- `node_modules/**/*` - Dependencies
- `.github/**/*` - GitHub workflows
- `jest.config.js` - Test configuration
- `tsconfig.json` - TypeScript configuration
