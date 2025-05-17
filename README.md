# Stock Balance Alert

This repository contains a GitHub Actions workflow that monitors a Google Sheet for changes in stock balance and sends alerts to Google Space when changes are detected.

## Setup

1. **Google Service Account**
   - Use your existing service account credentials
   - Add the service account JSON as a GitHub secret named `GOOGLE_SERVICE_ACCOUNT`
   - Share the Google Sheet with the service account email

2. **Google Space Webhook**
   - Create a webhook in your Google Space
   - Add the webhook URL as a GitHub secret named `SPACE_WEBHOOK_URL`

## How it Works

- The workflow runs every 5 minutes
- It checks the Google Sheet for any changes in stock balance
- If changes are detected, it sends an alert to Google Space
- The alert includes the specification and the change in balance
- Previous state is maintained between runs using a pickle file

## Manual Trigger

You can manually trigger the workflow using the "Actions" tab in GitHub.

## Environment Variables

The following secrets need to be set in GitHub:

- `GOOGLE_SERVICE_ACCOUNT`: The service account JSON credentials
- `SPACE_WEBHOOK_URL`: The webhook URL for Google Space 