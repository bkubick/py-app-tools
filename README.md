# Python Application Tools (py-app-tools)

## Description
Custom libraries I have built that can be used for different types of python web applications.


## Tools

### pynamo-migrate
An Alembic-like libary dedicated for handling DynamoDb migrations.

#### Commands

- `revision <message of the revision>`: Add a new migration revision
- `upgrade <version or head>`: Upgrade the dynamodb tables to the desired version, or head.
- `downgrade <number of downgrade versions>`: Downgrade the dynamodb tables the number of revisions. 
