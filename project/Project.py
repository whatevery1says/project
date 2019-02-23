"""Project.py."""

import json
import re
import os
import pymongo
import zipfile
from bson import BSON, Binary, json_util, ObjectId
from datetime import datetime
from io import BytesIO
from pymongo import MongoClient
from shutil import copytree, ignore_patterns, rmtree

from config import config

# Set up the MongoDB client, configure the databases, and assign variables to the "collections"
client = MongoClient(config.MONGO_CLIENT)
db = client.we1s
projects_db = db.Projects
corpus_db = db.Corpus

JSON_UTIL = json_util.default

class Project():
    """Model a project.

    Parameters:
    - manifest: dict containing form data for the project manifest.
    - templates_dir: the path to the templates
    - workspace_dir: the path where the project folder is to be saved

    Returns a JSON object with the format `{'response': 'success|fail', 'errors': []}`.

    """

    def __init__(self, manifest, templates_dir, workspace_dir):
        """Initialize the object."""
        self.manifest = manifest
        self.templates_dir = templates_dir
        self.workspace_dir = workspace_dir
        self.reduced_manifest = self.clean(manifest)
        self.name = manifest['name']
        if '_id' in self.reduced_manifest:
            self._id = self.reduced_manifest['_id']

    def clean(self, manifest):
        """Get a reduced version of the manifest, removing empty values."""
        data = {}
        for k, v in manifest.items():
            if v not in ['', [], ['']] and not k.startswith('builder_'):
                data[k] = v
        return data

    def copy(self, name, version=None):
        """Insert a copy of the current project into the database using a new name and _id.

        If no version is supplied, the latest version is used. Regardless, the new project
        is reset to version 1.
        """
        # Delete the _id and rename the project
        del self.reduced_manifest['_id']
        self.reduced_manifest['name'] = name
        # Get the latest version number if none is supplied
        if version == None:
            version = self.get_latest_version_number()
        # Get the version dict and reset the version number and date
        version_dict = self.get_version(version)
        version_dict['version_number'] = 1
        now = datetime.today().strftime('%Y%m%d%H%M%S')
        version_dict['version_date'] = now
        version_dict['version_name'] = now + '_v1_' + self.reduced_manifest['name']
        self.reduced_manifest['content'] = [version_dict]
        # Save the manifest
        try:
            projects_db.insert_one(self.reduced_manifest)
            return json.dumps({'result': 'success', 'project_dir': version_dict['version_name'], 'errors': []})
        except pymongo.errors.OperationFailure as e:
            print(e.code)
            print(e.details)
            return json.dumps({'result': 'fail', 'errors': ['<p>Unknown error: Could not insert the new project from the database.</p>']})

    def copy_templates(self, templates, project_dir):
        """Copy the workflow templates from the templates folder to the a project folder."""
        try:
            copytree(templates, project_dir, ignore=ignore_patterns('.ipynb_checkpoints', '__pycache__'))
            return []
        except IOError:
            return '<p>Error: The templates could not be copied to the project directory.</p>'


    def delete(self, version=None):
        """Delete a project or a project version, if the number is supplied."""
        if version == None:
            try:
                result = projects_db.delete_one({'_id': ObjectId(self._id)})
                if result.deleted_count > 0:
                    return {'result': 'success', 'errors': []}
                else:
                    return {'result': 'fail', 'errors': ['<p>Unknown error: Could not delete the project from the database.</p>']}
            except pymongo.errors.OperationFailure as e:
                print(e.code)
                print(e.details)
                return {'result': 'fail', 'errors': ['<p>Unknown error: Could not delete the project from the database.</p>']}
        else:
            try:
                for index, item in enumerate(self.reduced_manifest['content']):
                    if item['version_number'] == version:
                        del self.reduced_manifest['content'][index]
                        projects_db.update_one({'_id': ObjectId(self._id)},
                                   {'$set': {'content': self.reduced_manifest['content']}}, upsert=False)
            except pymongo.errors.OperationFailure as e:
                print(e.code)
                print(e.details)
                return {'result': 'fail', 'errors': ['<p>Unknown error: Could not delete the project from the database.</p>']}

    def exists(self):
        """Test whether the project already exists in the database."""
        test = projects_db.find_one({'_id': ObjectId(self._id)})
        # test = projects_db.find_one({'name': self.name}) # For testing
        if test is not None:
            return True
        return False

    def export(self, version=None):
        """Export a project in the Workspace."""
        errors = []
        # Get the version dict. Use the latest version if no version number is supplied.
        if version == None:
            version_dict = self.get_latest_version()
        else:
            version_dict = self.get_version(version)
        # Set the filename and path to write to
        zipname = version_dict['version_name'] + '.zip'
        exports_dir = self.workspace_dir + '/' + 'exports'
        # 1. First try to get the zip file from the manifest and copy it to the exports folder
        if 'version_zipfile' in version_dict:
            try:
                zipfile = version_dict['version_zipfile']
                with open(os.path.join(exports_dir, zipname), 'wb') as f:
                    f.write(zipfile)
            except IOError:
                errors.append('Error: Could not write the zip archive to the exports directory.')
        # 2. Next try to find an active project folder in the Workspace and zip it to the exports folder
        elif os.path.exists(os.path.join(self.workspace_dir, version_dict['version_name'])):
            try:
                project_dir = os.path.join(self.workspace_dir, version_dict['version_name'])
                zipfile = self.zip(zipname, project_dir, exports_dir)
            except IOError:
                errors.append('Error: Could not zip project folder from the Workspace to the exports directory.')
        # 3. Finally, create a new project folder in the exports directory and populate from the database
        else:
            # Make a version folder in the exports directory
            project_dir = exports_dir + '/' + version_dict['version_name']
            os.makedirs(project_dir, exist_ok=True)
            # Get the data and put a manifest in it and write data to the caches/json folder
            self.reduced_manifest['db_query'] = json.loads('{"$and":[{"metapath":"Corpus,guardian,RawData"}]}')
            result = corpus_db.find(self.reduced_manifest['db_query'])
            try:
                with open(os.path.join(project_dir, 'datapackage.json'), 'w') as f:
                    f.write(json.dumps(self.reduced_manifest, indent=2, sort_keys=False, default=JSON_UTIL))
                json_caches = os.path.join(project_dir, 'caches/json')
                os.makedirs(json_caches, exist_ok=True)
                for item in result:
                    filename = os.path.join(json_caches, item['name'] + '.json')
                    with open(filename, 'w') as f:
                        f.write(json.dumps(item, indent=2, sort_keys=False, default=JSON_UTIL))
            except IOError:
                errors.append('<p>Error: Could not write data files to the caches directory.</p>')
            # Zip up the project folder, then delete the folder
            zipfile = self.zip(zipname, project_dir, exports_dir)
            rmtree(project_dir)
        # Return the path to the zip archive
        if len(errors) > 0:
            return json.dumps({'result': 'fail', 'errors': errors})
        else:
            return json.dumps({'result': 'success', 'filepath': exports_dir + '/' + zipname, 'errors': errors})

    def get_latest_version_number(self):
        """Get the latest version number from the versions dict.

        Returns an integer or 1, if no version information is available.
        """
        version_numbers = []
        if 'content' in self.reduced_manifest and len(self.reduced_manifest['content']) > 0:
            for version in self.reduced_manifest['content']:
                version_numbers.append(int(version['version_number']))
            _latest_version_number = max(version_numbers)
        else:
            _latest_version_number = 1
        return _latest_version_number

    def get_latest_version(self):
        """Get the dict for the latest version."""
        _latest_version = {}
        version_numbers = []
        if 'content' in self.reduced_manifest and len(self.reduced_manifest['content']) > 0:
            for version in self.reduced_manifest['content']:
                version_numbers.append(int(version['version_number']))
            _latest_version_number = max(version_numbers)
            for version in self.reduced_manifest['content']:
                if version['version_number'] == _latest_version_number:
                    _latest_version = version['version_number']
        return self.get_version(_latest_version)


    def get_version(self, value, key='number'):
        """Get the dict for a specific version.

        Accepts an integer version number by default. If the key is 'number',
        'name', or 'date', that value is used to find the dict.
        """
        if 'content' in self.reduced_manifest:
            versions = self.reduced_manifest['content']
            for version in versions:
                if version['version_' + key] == value:
                    return version
        else:
            raise ValueError('No versions were included in the manifest.')

    def launch(self, workflow, version=None, new=True):
        """Prepare the project in the Workspace.

        If the user does not have any datapackages stored in the database, a new v1
        project_dir is created. Otherwise, if the user clicks the main rocket icon,
        a new project_dir is created based on the latest version. If the user clicks
        on a specific version's rocket icon, a project_dir based on that version's
        datapackage is created. Where possible, a datapackage is unzipped to the
        Workspace. Otherwise, the data is written to the project_dir from the database.
        """
        # If the manifest has a zipfile, skip Option 1
        if 'content' in self.reduced_manifest:
            for item in self.reduced_manifest['content']:
                if 'zipfile' in item:
                    version = 'latest'

        # Get a timestamp
        now = datetime.today().strftime('%Y%m%d%H%M%S')

        # Option 1. Generate a project_dir for a new v1
        if new == True and version == None:
            version_name = now + '_v1_' + self.reduced_manifest['name']
            self.reduced_manifest['content'] = {
                'version_date': now,
                'version_number': 1,
                'version_name': version_name,
                'version_workflow': workflow
            }
            project_dir = os.path.join(self.workspace_dir, version_name)
            templates = os.path.join(self.templates_dir, workflow)
            errors = self.make_new_project_dir(project_dir, templates)
            if errors == []:
                return json.dumps({'result': 'success', 'project_dir': project_dir, 'errors': []})
            else:
                return json.dumps({'result': 'fail', 'errors': errors})

        # Option 2. Generate a new project_dir based on the latest version
        if new == True and version is not None:
            version_dict = self.get_latest_version()
            next_version_number = version_dict['version_number'] + 1
            next_version_name = now + '_v' + str(next_version_number) +'_' + self.reduced_manifest['name']
            next_version = {
                'version_date': now,
                'version_number': next_version_number,
                'version_workflow': workflow,
                'version_name': next_version_name,
                'version_zipfile': version_dict['version_zipfile']
            }
            self.reduced_manifest['content'] = next_version
            project_dir = os.path.join(self.workspace_dir, next_version_name)
            try:
                self.unzip(version_dict['version_zipfile'], project_dir)
                return json.dumps({'result': 'success', 'project_dir': project_dir, 'errors': []})
            except:
                errors = ['<p>Unknown error: Could not unzip the project datapackage to the project directory.</p>']
                return json.dumps({'result': 'fail', 'errors': errors})

        # Option 3. Launch a specific version
        if new == False:
            if version == None:
                version_dict = self.get_latest_version()
            else:
                version_dict = self.get_version(version)
            print('Launching ' + version_dict['version_name'])
            project_dir = os.path.join(self.workspace_dir, version_dict['version_name'])
            # If the project is live in the workspace, return a link to the folder
            if os.path.exists(project_dir):
                return json.dumps({'result': 'success', 'project_dir': project_dir, 'errors': []})
            # Otherwise, unzip the datapackage from the manifest to the workspace
            else:
                try:
                    self.unzip(version_dict['version_zipfile'], project_dir, binary=True)
                    print('Unzipped to ' + project_dir)
                except:
                    errors.append('<p>Unknown error: Could not unzip the project datapackage to the project directory.</p>')
                    return json.dumps({'result': 'fail', 'errors': errors})

    def make_new_project_dir(self, project_dir, templates):
        """Provide a helper function for Project.launch()."""
        errors = []
        # The project_dir must not already exist
        error = self.copy_templates(templates, project_dir)
        if error != []:
            errors.append(error)
        # If the there is a db_query, get the data
        self.reduced_manifest['db_query'] = json.loads('{"$and": [{"metapath":"Corpus,guardian,RawData"}]}')
        if 'db_query' in self.reduced_manifest:
            try:
                result = list(corpus_db.find(self.reduced_manifest['db_query']))
                if len(result) == 0:
                    errors.append('<p>The database query returned no results.</p>')
            except pymongo.errors.OperationFailure as e:
                print(e.code)
                print(e.details)
                msg = '<p>Unknown Error: The database query could not be executed.</p>'
                errors.append(msg)
            # Write the data manifests to the caches/json folder
            try:
                json_caches = os.path.join(project_dir, 'caches/json')
                os.makedirs(json_caches, exist_ok=True)
                with open(os.path.join(project_dir, 'datapackage.json'), 'w') as f:
                    f.write(json.dumps(self.reduced_manifest, indent=2, sort_keys=False, default=JSON_UTIL))

                for item in result:
                    filename = os.path.join(json_caches, item['name'] + '.json')
                    with open(filename, 'w') as f:
                        f.write(json.dumps(item, indent=2, sort_keys=False, default=JSON_UTIL))
            except IOError:
                errors.append('<p>Error: Could not write data files to the caches directory.</p>')
        else:
            errors.append('<p>Please enter a database query in the Data Resources tab.</p>')

        return errors

    def parse_version(self, s, output=None):
        """Separate a project folder name into its component parts.

        The output argument allows you to return a single component.
        """
        version = re.search('(.+)_v([0-9]+)_(.+)', s)
        if output == 'date':
            return version.group(1)
        elif output == 'number':
            return version.group(2)
        elif output == 'name':
            return version.group(3)
        else:
            return version.group(1), version.group(2), version.group(3)

    def print_manifest(self):
        """Print the manifest."""
        print(json.dumps(self.reduced_manifest, indent=2, sort_keys=False, default=JSON_UTIL))

    def save(self, new=False):
        """Save a  project in the database."""
        # Note: new parameter is not yet handled
        action = 'inserted'
        project_exists = self.exists()
        if project_exists:
            action = 'updated'
        try:
            if project_exists == True:
                print('The project already exists. Updating...')
                projects_db.update_one({'_id': ObjectId(self._id)},
                                    {'$set': self.reduced_manifest}, upsert=False)
            else:
                print('The project does not exist. Inserting...')
                now = datetime.today().strftime('%Y%m%d%H%M%S')
                self.reduced_manifest['content'] = {
                    'version_date': now,
                    'version_number': 1,
                    'version_name': now + '_v1_' + self.reduced_manifest['name'],
                    'version_workflow': None
                }
                projects_db.insert_one(self.reduced_manifest)
            return {'result': 'success', 'errors': []}
        except pymongo.errors.OperationFailure as e:
            print(e.code)
            print(e.details)
            msg = 'Unknown Error: The record for <code>name</code> <strong>' + \
                self.name + '</strong> could not be ' + action + '.'
            return {'result': 'fail', 'errors': [msg]}

    ## The next two methods need to be incorporated into save()

    def save_new_version(self, workflow='None', zipfile='None'):
        """Save a new version to a project."""
        try:
            now = datetime.today().strftime('%Y%m%d%H%M%S')
            new_version = {
                    'version_date': now,
                    'version_number': self.get_latest_version_number() + 1,
                    'version_name': now + '_v1_' + self.reduced_manifest['name'],
                    'version_workflow': workflow
                }
            # NB. This does not have the actual zip file
            self.reduced_manifest['content'].append(new_version)
            projects_db.update_one({'_id': ObjectId(self._id)},
                                {'$set': self.reduced_manifest}, upsert=False)
            return {'result': 'success', 'errors': []}
        except pymongo.errors.OperationFailure as e:
            print(e.code)
            print(e.details)
            msg = 'Unknown Error: The record for <code>name</code> <strong>' + \
                self.name + '</strong> could not be updated.'
            return {'result': 'fail', 'errors': [msg]}

    def save_as(self, name, workflow='None', zipfile='None'):
        """Save a copy of a project with a new name."""
        try:
            now = datetime.today().strftime('%Y%m%d%H%M%S')
            self.reduced_manifest['content'] = {
                    'version_date': now,
                    'version_number': 1,
                    'version_name': now + '_v1_' + self.reduced_manifest['name'],
                    'version_workflow': workflow
                }
            projects_db.insert_one(self.reduced_manifest)
            return {'result': 'success', 'errors': []}
        except pymongo.errors.OperationFailure as e:
            print(e.code)
            print(e.details)
            msg = 'Unknown Error: The record for <code>name</code> <strong>' + \
                self.name + '</strong> could not be updated.'
            return {'result': 'fail', 'errors': [msg]}

    def unzip(self, source=None, output_path=None, binary=False):
        """Unzip the specified file to a project folder in the Workspace.

        Uses the current path if one is not specified.
        """
        if binary == True:
            # Copy the zip archive to memory; then unzip it to the output path
            temp_zipfile = zipfile.ZipFile(BytesIO(source))
            temp_zipfile.extractall(output_path)
            return {'result': 'success', 'output_path': output_path, 'errors': []}
        # Otherwise, the source is a filepath
        else:
            try:
                with zipfile.ZipFile(source, 'r') as zip_ref:
                    zip_ref.extractall(output_path)                
                return {'result': 'success', 'output_path': output_path, 'errors': []}
            except:
                return {'result': 'fail', 'errors': ['<p>Could not unzip the file at ' + source + '.</p>']}


    def zip(self, filename, source_dir, destination_dir):
        """Create a zip archive of the project folder and writes it to the destination folder."""
        errors = []
        try:
            if not os.path.exists(destination_dir):
                os.makedirs(destination_dir)
        except:
            errors.append('<p>The destination directory does not exist, and it could not be created.</p>')
            return {'result': 'fail', 'errors': errors}
        try:
            zip_path = os.path.join(destination_dir, filename)
            zipobj = zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED)
            rootlen = len(source_dir) + 1
            for base, _, files in os.walk(source_dir):
                for file in files:
                    fn = os.path.join(base, file)
                    zipobj.write(fn, fn[rootlen:])
            return {'result': 'success', 'zip_path': zip_path, 'errors': []}
        except:
            errors.append('<p>Unknown error: a zip archive could not be created with the supplied source directory and filename.</p>')
            return {'result': 'fail', 'errors': errors}

# Send feedback to the notebook cell
print('Project module loaded.')
