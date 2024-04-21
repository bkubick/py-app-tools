"""Manage DynamoDB migrations.

NOTE: This script is a simplified version of Alembic for DynamoDB. It is used to manage
the migrations for the DynamoDB database.

The script supports the following commands:
    - upgrade: Upgrade the migration to the given version.
        version: The version to upgrade to.
    - downgrade: Downgrade the migration by the given number of steps.
        number_of_steps: The number of steps to downgrade.
    - revision: Create a new revision migration file.
        name: The name of the migration.
"""
from __future__ import annotations

import argparse
import os
import typing

import boto3

if typing.TYPE_CHECKING:
    from mypy_boto3_dynamodb import DynamoDBServiceResource


# Constants
VERSION_TABLE = 'DynamoDBMigrationVersion'
VERSIONS_MODULE = 'migrations.versions'
VERSIONS_DIR = './migrations/versions'
VERSION_TEMPLATE = './migrations/version_template.txt'

# AWS DynamoDB
ENDPOINT_URL = "<Define the NoSQL database URL>"
REGION_NAME = "<Define the AWS region>"
AWS_ACCESS_KEY_ID = "<Define the AWS access key ID>"
AWS_SECRET_ACCESS_KEY = "<Define the AWS secret access key>"

# DynamoDB
ddb: DynamoDBServiceResource = boto3.resource('dynamodb',
    endpoint_url=ENDPOINT_URL,
    region_name=REGION_NAME,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)


def _arg_parser() -> argparse.Namespace:
    """Parse the command line arguments.

    Parsed Arguments:
        upgrade: Upgrade the migration to the given version.
            version: The version to upgrade to.
        downgrade: Downgrade the migration by the given number of steps.
            number_of_steps: The number of steps to downgrade.
        revision: Create a new revision migration file.
            name: The name of the migration.

    Returns:
        argparse.Namespace: The command line arguments.
    """
    parser = argparse.ArgumentParser(description='Manage DynamoDB migrations')
    subparsers = parser.add_subparsers(dest='command')

    upgrade_parser = subparsers.add_parser('upgrade', help='Upgrade the migration')
    upgrade_parser.add_argument('version', nargs='?', default='head', help='The version to upgrade to')

    downgrade_parser = subparsers.add_parser('downgrade', help='Downgrade the migration')
    downgrade_parser.add_argument('number_of_steps', nargs='?', default=1, type=int, help='The number of steps to downgrade')

    revision_parser = subparsers.add_parser('revision', help='Create a new revision migration file')
    revision_parser.add_argument('name', help='The name of the migration')

    return parser.parse_args()


def _create_version_table() -> None:
    """Create a table in DynamoDB to store the version the migration is at.

    This table will have a single item with a single attribute, the version number.
    """
    ddb.create_table(
        TableName=VERSION_TABLE,
        AttributeDefinitions=[
            {
                'AttributeName': 'id',
                'AttributeType': 'N'
            }
        ],
        KeySchema=[
            {
                'AttributeName': 'id',
                'KeyType': 'HASH'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 1,
            'WriteCapacityUnits': 1
        }
    )


def _set_db_version(version: int) -> None:
    """Set the version of the migration in the database.

    Args:
        version (int): The version to set.
    """
    table = ddb.Table(VERSION_TABLE)

    try:
        table.update_item(
            Key={'id': 1},
            AttributeUpdates={
                'version': version,
            },
        )
    except Exception as e:
        table.put_item(Item={'id': 1, 'version': version})


def _get_db_version() -> int:
    """Get the current version of the migration.

    Returns:
        int: The current version of the migration.
    """
    table = ddb.Table(VERSION_TABLE)
    response = table.scan()
    items = response.get('Items')
    return int(items[-1].get('version')) if items else 0


def _get_migration_filenames() -> typing.List[str]:
    """Get the filenames of the migrations in alphabetical order.

    i.e. ['1_initial_table.py', '2_add_new_table.py', ...]

    Returns:
        typing.List[str]: The filenames of the migrations.
    """
    migrations = []
    for filename in os.listdir(VERSIONS_DIR):
        if filename.endswith('.py'):
            migrations.append(filename)

    return sorted(migrations)


def _migrate_to_version(version: int, is_upgrade: bool) -> None:
    """Migrate to the given version.

    Args:
        version (int): The version to migrate to.
    """
    migration_filenames = _get_migration_filenames()
    current_db_version = _get_db_version()

    if not is_upgrade:
        if version - 1 >= 0:
            run_migrations = migration_filenames[current_db_version-1:version-1:-1]
        else:
            run_migrations = migration_filenames[current_db_version-1::-1]
    else:
        run_migrations = migration_filenames[current_db_version:version]

    for filename in run_migrations:
        print(f'{"Upgrade" if is_upgrade else "Downgrade"} file: ', filename)

        version_number = int(filename.split('_')[0])
        module_name = filename.replace('.py', '')
        module = __import__(f'{VERSIONS_MODULE}.{module_name}', fromlist=[module_name])

        if is_upgrade:
            module.upgrade()
            migrated_version = version_number
        else:
            module.downgrade()
            migrated_version = version_number - 1

        _set_db_version(migrated_version)


def upgrade(version: typing.Union[str, int]) -> None:
    """Upgrade the migration to the given version.
    
    Args:
        version (str, optional): The version to upgrade to. Defaults to 'head'.
    """
    # Create the version table if it doesn't exist
    try:
        _create_version_table()
    except Exception as e:
        pass

    migrations = _get_migration_filenames()
    current_version = _get_db_version()

    if version == 'head':
        version = migrations[-1].split('_')[0]
    elif not version.isdigit() or int(version) < 0:
        print('Invalid version')
        return
    elif int(version) > len(migrations):
        print('Version not found')
        return
    elif int(version) <= int(current_version):
        print(f'Version is already up to date - Current Version {current_version}')
        return

    _migrate_to_version(int(version), is_upgrade=True)
    

def downgrade(number_of_steps: int) -> None:
    """Downgrade the migration by the given number of steps.

    Args:
        number_of_steps (int, optional): The number of steps to downgrade.
    """
    current_db_version = _get_db_version()
    desired_version = int(current_db_version) - number_of_steps

    if number_of_steps < 0:
        print('Invalid number of steps')
        return
    elif desired_version < 0:
        print('Cannot downgrade below version 0')
        return

    _migrate_to_version(desired_version, is_upgrade=False)

def revision(name: str) -> None:
    """Create a new revision migration file with the given name.

    Args:
        name (str): The name of the migration.
    """
    migration_filenames = _get_migration_filenames()
    version_head = int(migration_filenames[-1].split('_')[0])
    new_version = version_head + 1

    snake_name = name.lower().replace(' ', '_')
    filename = f'{new_version}_{snake_name}.py'
    
    with open(VERSION_TEMPLATE, 'r') as f:
        template = f.read()
    
    with open(f'{VERSIONS_DIR}/{filename}', 'w') as f:
        f.write(template.replace('<revision_number>', str(new_version)).replace('<revision_description>', name))

    print(f'Successfully created migration file {filename}')


def create_versions_dir() -> None:
    """Setup the PynamoDB migration structure."""
    if not os.path.exists(VERSIONS_DIR):
        os.makedirs(VERSIONS_DIR)

        print('Successfully setup PynamoDB migration structure')
    else:
        print('PynamoDB migration structure already exists')


if __name__ == '__main__':
    args = _arg_parser()

    if args.command == 'upgrade':
        upgrade(args.version)
    elif args.command == 'downgrade':
        downgrade(args.number_of_steps)
    elif args.command == 'revision':
        revision(args.name)
    else:
        print('Invalid command')
