# Issue Triage and Workflow Guide

This document outlines the conventions and processes for managing issues and pull requests in the Handout project.

## Issue Labels

### Priority Labels

- `priority:critical` - Critical bugs or issues that block releases
- `priority:high` - Important features or bugs that should be addressed soon
- `priority:medium` - Standard priority for most tasks
- `priority:low` - Nice-to-have improvements or minor bugs

### Type Labels

- `type:bug` - Something isn't working as expected
- `type:enhancement` - New feature or request
- `type:documentation` - Documentation improvements
- `type:question` - Further information is requested
- `type:refactoring` - Code change that neither fixes a bug nor adds a feature
- `type:performance` - Performance improvements
- `type:test` - Test-related changes

### Status Labels

- `status:blocked` - Blocked by another issue
- `status:in-progress` - Work in progress
- `status:needs-review` - Ready for review
- `status:needs-information` - Awaiting more information from reporter
- `status:duplicate` - Duplicate of another issue
- `status:wontfix` - Won't be worked on for reasons mentioned in comments

### Component Labels

- `comp:core` - Core DAG functionality
- `comp:distributed` - Distributed execution system
- `comp:resources` - Resource tracking and allocation
- `comp:visualization` - Visualization tools
- `comp:api` - Public API
- `comp:examples` - Example applications and usage
- `comp:telemetry` - Telemetry and monitoring

### Difficulty Labels

- `difficulty:starter` - Good for new contributors
- `difficulty:intermediate` - Requires familiarity with the codebase
- `difficulty:advanced` - Complex issues requiring deep knowledge

## Milestones

Milestones are used to group issues for specific releases or significant project phases:

- Version milestones (e.g., `v0.2.0`, `v1.0.0`)
- Feature-based milestones (e.g., `Core API Stability`, `UI Components`)

## Issue Lifecycle

1. **New Issue**
   - Newly created issues are unassigned and unlabeled
   - Maintainers triage within 3 business days

2. **Triage Process**
   - Add appropriate labels
   - Assign to milestone if applicable
   - Request more information if needed
   - Close if duplicate/invalid/wontfix

3. **In Progress**
   - Issue is assigned to a contributor
   - `status:in-progress` label is added
   - Updates provided through comments

4. **Review**
   - PR is submitted
   - `status:needs-review` label is added
   - Reviewers provide feedback

5. **Completion**
   - PR is merged
   - Issue is closed
   - Referenced in CHANGELOG.md

## Pull Request Workflow

1. **Preparation**
   - Create an issue for the PR to reference
   - Discuss implementation approach if needed

2. **Creation**
   - Follow [PR guidelines](../CONTRIBUTING.md#pull-request-process)
   - Reference the issue number in the PR description
   - Add appropriate labels

3. **Review Process**
   - CI checks must pass
   - At least one maintainer approval is required
   - Address all requested changes

4. **Merge Requirements**
   - All discussions resolved
   - CI passing
   - Required approvals obtained
   - Up-to-date with main branch

5. **Post-Merge**
   - Close related issues
   - Update documentation if necessary

## Release Planning

Issues are planned for releases by assigning them to version milestones.

### Versioning Criteria

- **PATCH** releases (0.x.1): Bug fixes and minor improvements
- **MINOR** releases (0.x.0): New features, backward-compatible
- **MAJOR** releases (x.0.0): API changes, significant architecture changes

## Maintenance Rotation

Maintainers rotate weekly responsibility for:

1. Initial triage of new issues
2. Review of pending PRs
3. Community assistance
4. Release coordination

## Automation

The repository uses GitHub Actions for automating:

- Labeling issues based on title/content
- PR validation
- Status changes based on activity
- Stale issue handling
- Release notes generation

## Decision Making

Technical decisions follow this process:

1. Issue discussion to outline alternatives
2. Proposal submitted as a PR/issue
3. Feedback period (1 week minimum)
4. Consensus-based decision
5. Documentation of decision and rationale
