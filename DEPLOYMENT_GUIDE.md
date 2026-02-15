# Deployment Guide

## GitHub Secrets Setup

To enable automated deployments via GitHub Actions, configure the following secrets in your GitHub repository:

### Required Secrets

1. **DATABRICKS_HOST**
   - Your Databricks workspace URL
   - Example: `https://dbc-50a782d7-ee0e.cloud.databricks.com/`

2. **DATABRICKS_TOKEN**
   - Personal Access Token from Databricks
   - Generate from: Databricks Workspace → User Settings → Access Tokens

### How to Add Secrets

1. Go to your GitHub repository
2. Navigate to **Settings** → **Secrets and variables** → **Actions**
3. Click **New repository secret**
4. Add each secret with the name and value as specified above

## Deployment Workflows

### Dev Environment
- **Trigger**: Push to `dev` branch
- **Target**: Development workspace
- **Workflow**: `.github/workflows/deploy-dev.yml`

### Prod Environment
- **Trigger**: Push/merge to `main` branch
- **Target**: Production workspace
- **Workflow**: `.github/workflows/deploy-prod.yml`

## Manual Deployment

For local deployment with TLS skip (if needed):

```bash
# Deploy to dev
DATABRICKS_INSECURE_TLS_SKIP_VERIFY=true databricks bundle deploy -t dev

# Deploy to prod
DATABRICKS_INSECURE_TLS_SKIP_VERIFY=true databricks bundle deploy -t prod
```

## Environment Variables

Set these in your local environment for manual deployments:

```bash
export DATABRICKS_HOST="https://your-workspace.azuredatabricks.net"
export DATABRICKS_TOKEN="your-token-here"
```

