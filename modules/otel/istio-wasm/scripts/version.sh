#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODULE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$MODULE_DIR"

echo "OTEL Istio Wasm Module Version Management"
echo "================================"

# Function to print colored output
print_status() {
    echo -e "\033[1;34m[INFO]\033[0m $1"
}

print_success() {
    echo -e "\033[1;32m[SUCCESS]\033[0m $1"
}

print_error() {
    echo -e "\033[1;31m[ERROR]\033[0m $1"
}

print_warning() {
    echo -e "\033[1;33m[WARN]\033[0m $1"
}

sed_in_place() {
    if [[ "$OSTYPE" == darwin* ]]; then
        sed -i "" "$@"
    else
        sed -i "$@"
    fi
}

# Show help
show_help() {
    cat << EOF
Usage: $0 COMMAND [OPTIONS]

Manage versions for the OpenTelemetry Istio Wasm module.

COMMANDS:
    current                 Show current version
    bump VERSION           Update version in project files
    update-manifests VER   Update deployment manifests with version
    tag VERSION            Create and push git tag
    release VERSION        Complete release process

OPTIONS:
    -h, --help             Show this help message
    --dry-run              Show what would be done without making changes

EXAMPLES:
    $0 current                      # Show current version
    $0 bump v1.2.0                 # Update to version 1.2.0
    $0 update-manifests v1.2.0     # Update manifests only
    $0 release v1.2.0              # Complete release process

EOF
}

# Get current version from Cargo.toml
get_current_version() {
    if [ -f "Cargo.toml" ]; then
        grep '^version = ' Cargo.toml | head -n1 | cut -d'"' -f2
    else
        print_error "Cargo.toml not found"
        exit 1
    fi
}

# Update version in Cargo.toml
update_cargo_version() {
    local version="$1"
    local clean_version="${version#v}"  # Remove 'v' prefix if present
    
    if [ -f "Cargo.toml" ]; then
        sed_in_place "s/^version = .*/version = \"$clean_version\"/" Cargo.toml
        print_success "Updated Cargo.toml to version $clean_version"
    else
        print_error "Cargo.toml not found"
        exit 1
    fi
}

# Update deployment manifests with new version tag
update_deployment_manifests() {
    local version="$1"
    local updated=0
    
    # Update minimal.yaml
    if [ -f "deploy/minimal.yaml" ]; then
        sed_in_place "s|oci://.*softprobe/softprobe:.*|oci://docker.io/softprobe/otel-istio-wasm:$version|; s|oci://.*softprobe/otel-istio-wasm:.*|oci://docker.io/softprobe/otel-istio-wasm:$version|" deploy/minimal.yaml
        print_success "Updated deploy/minimal.yaml"
        updated=$((updated + 1))
    fi
    
    # Update production.yaml
    if [ -f "deploy/production/sp-istio-agent.yaml" ]; then
        sed_in_place "s|oci://.*softprobe/softprobe:.*|oci://docker.io/softprobe/otel-istio-wasm:$version|; s|oci://.*softprobe/otel-istio-wasm:.*|oci://docker.io/softprobe/otel-istio-wasm:$version|" deploy/production/sp-istio-agent.yaml
        print_success "Updated deploy/production/sp-istio-agent.yaml"
        updated=$((updated + 1))
    fi

    if [ -f "deploy/production/test-bookinfo.yaml" ]; then
        sed_in_place "s|oci://.*softprobe/softprobe:.*|oci://docker.io/softprobe/otel-istio-wasm:$version|; s|oci://.*softprobe/otel-istio-wasm:.*|oci://docker.io/softprobe/otel-istio-wasm:$version|" deploy/production/test-bookinfo.yaml
        print_success "Updated deploy/production/test-bookinfo.yaml"
        updated=$((updated + 1))
    fi
    
    # Update examples
    for file in deploy/examples/*.yaml; do
        if [ -f "$file" ] && grep -q "softprobe" "$file"; then
            sed_in_place "s|oci://softprobe/otel-istio-wasm:.*|oci://softprobe/otel-istio-wasm:$version|" "$file"
            print_success "Updated $file"
            updated=$((updated + 1))
        fi
    done
    
    if [ $updated -eq 0 ]; then
        print_warning "No deployment manifests found to update"
    else
        print_success "Updated $updated deployment manifest(s)"
    fi
}

# Create and push git tag
create_git_tag() {
    local version="$1"
    local tag="otel-istio-wasm-${version#otel-istio-wasm-}"
    
    # Check if we're in a git repository
    if ! git rev-parse --git-dir > /dev/null 2>&1; then
        print_error "Not in a git repository"
        exit 1
    fi
    
    # Check if tag already exists
    if git tag -l | grep -q "^$tag$"; then
        print_error "Tag $tag already exists"
        exit 1
    fi
    
    # Check for uncommitted changes
    if ! git diff --quiet || ! git diff --cached --quiet; then
        print_warning "You have uncommitted changes. Consider committing them first."
        read -p "Continue anyway? (y/N): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            exit 1
        fi
    fi
    
    # Create annotated tag
    git tag -a "$tag" -m "Release $tag"
    print_success "Created tag $tag"
    
    # Push tag
    git push origin "$tag"
    print_success "Pushed tag $tag to origin"
}

# Validate version format
validate_version() {
    local version="$1"
    
    if [[ ! "$version" =~ ^v?[0-9]+\.[0-9]+\.[0-9]+(-[a-zA-Z0-9.-]+)?$ ]]; then
        print_error "Invalid version format: $version"
        print_error "Expected format: v1.2.3 or 1.2.3 (with optional pre-release suffix)"
        exit 1
    fi
}

# Complete release process
do_release() {
    local version="$1"
    
    print_status "Starting release process for version $version"
    
    # Validate version
    validate_version "$version"
    
    # Update files
    update_cargo_version "$version"
    update_deployment_manifests "$version"
    
    # Build and run the module's integration harness
    print_status "Building and validating the integration harness..."
    if ! make build; then
        print_error "Build failed"
        exit 1
    fi
    
    if ! make integration-test; then
        print_error "Integration validation failed"
        exit 1
    fi
    
    # Commit changes
    print_status "Committing version changes..."
    git add Cargo.toml deploy/
    git commit -m "Release OTEL Istio Wasm module $version

🤖 Generated with [Claude Code](https://claude.ai/code)

Co-Authored-By: Claude <noreply@anthropic.com>"
    
    # Create tag
    create_git_tag "$version"
    
    print_success "Release otel-istio-wasm-$version completed successfully!"
    print_status "Next steps:"
    print_status "1. Build and push Docker images: make docker-push VERSION=$version"
    print_status "2. Create GitHub release with release notes"
    print_status "3. Update documentation if needed"
}

# Main execution
DRY_RUN=false
COMMAND=""

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            show_help
            exit 0
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        current|bump|update-manifests|tag|release)
            COMMAND="$1"
            shift
            break
            ;;
        *)
            print_error "Unknown argument: $1"
            show_help
            exit 1
            ;;
    esac
done

# Check if command was provided
if [ -z "$COMMAND" ]; then
    print_error "Command required"
    show_help
    exit 1
fi

# Execute command
case "$COMMAND" in
    current)
        CURRENT=$(get_current_version)
        print_status "Current version: $CURRENT"
        ;;
    bump)
        if [ $# -ne 1 ]; then
            print_error "Version required for bump command"
            show_help
            exit 1
        fi
        VERSION="$1"
        validate_version "$VERSION"
        if [ "$DRY_RUN" = true ]; then
            print_status "DRY RUN: Would update version to $VERSION"
        else
            update_cargo_version "$VERSION"
            print_success "Version bumped to $VERSION"
        fi
        ;;
    update-manifests)
        if [ $# -ne 1 ]; then
            print_error "Version required for update-manifests command"
            show_help
            exit 1
        fi
        VERSION="$1"
        validate_version "$VERSION"
        if [ "$DRY_RUN" = true ]; then
            print_status "DRY RUN: Would update manifests to version $VERSION"
        else
            update_deployment_manifests "$VERSION"
        fi
        ;;
    tag)
        if [ $# -ne 1 ]; then
            print_error "Version required for tag command"
            show_help
            exit 1
        fi
        VERSION="$1"
        validate_version "$VERSION"
        if [ "$DRY_RUN" = true ]; then
            print_status "DRY RUN: Would create and push tag $VERSION"
        else
            create_git_tag "$VERSION"
        fi
        ;;
    release)
        if [ $# -ne 1 ]; then
            print_error "Version required for release command"
            show_help
            exit 1
        fi
        VERSION="$1"
        if [ "$DRY_RUN" = true ]; then
            print_status "DRY RUN: Would perform complete release for $VERSION"
        else
            do_release "$VERSION"
        fi
        ;;
    *)
        print_error "Unknown command: $COMMAND"
        show_help
        exit 1
        ;;
esac
