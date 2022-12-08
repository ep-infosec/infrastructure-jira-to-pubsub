# infrastructure-jira-to-pubsub - Jira to PubSub bridge service

This service bridges Jira and PyPubSub by polling for the latest changes to new and existing tickets in Jira, then sending the change payload to PyPubSub.
It runs as a PipService.
