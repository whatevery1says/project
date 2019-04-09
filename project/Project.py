"""Project.py."""

import glob
import hashlib
import json
import re
import os
import nbformat
import pymongo
import sys
import zipfile
from bson import BSON, Binary, json_util, ObjectId
from collections import defaultdict
from datetime import datetime
from io import BytesIO
from pymongo import MongoClient, ReturnDocument
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

    def clean_nb(self, nbfile, clean_outputs=True, clean_notebook_metadata_fields=None, clean_cell_metadata_fields=None, clean_tags=None, clean_empty_cells=False, save=False):
        """Clean metadata fields and outputs from notebook.

        Cleans outputs only by default.
        Takes a path to the notebook file. Returns a json object with the cleaned notebook.
        Based on nbtoolbelt: https://gitlab.tue.nl/jupyter-projects/nbtoolbelt/blob/master/src/nbtoolbelt/cleaning.py
        """
        # Read the file
        try:
            nb = nbformat.read(nbfile, as_version=4)
        except Exception as e:
            print('{}: {}'.format(type(e).__name__, e), sys.stderr)
        freq = defaultdict(int)  # number of cleanings
        # delete notebook (global) metadata fields
        if isinstance(clean_notebook_metadata_fields, list) and len(clean_notebook_metadata_fields) > 0:
            for field in clean_notebook_metadata_fields:
                if field in nb.metadata:
                    del nb.metadata[field]
                    freq['global ' + field] += 1
        # delete empty cells, if desired
        n = len(nb.cells)
        if clean_empty_cells:
            nb.cells = [cell for cell in nb.cells if self.count_source(cell.source)[0]]
        if n > len(nb.cells):
            freq['empty cells'] = n - len(nb.cells)
        # traverse all cells, and delete fields
        for _, cell in enumerate(nb.cells):  # Assign an index if needed for debugging
            ct = cell.cell_type
            # delete cell metadata fields
            if isinstance(clean_cell_metadata_fields, list) and len(clean_cell_metadata_fields) > 0:
                for field in clean_cell_metadata_fields:
                    if field in cell.metadata:
                        del cell.metadata[field]
                        freq['cell ' + field] += 1
            # delete cell tags
            if 'tags' in cell.metadata and isinstance(clean_tags, list) and len(clean_tags) > 0:
                removed_tags = {tag for tag in cell.metadata.tags if tag in clean_tags}
                for tag in removed_tags:
                    freq['tag ' + tag] += 1
                clean_tags = [tag for tag in cell.metadata.tags if tag not in clean_tags]
                if clean_tags:
                    cell.metadata.tags = clean_tags
                else:
                    del cell.metadata['tags']
            # clean outputs of code cells, if requested
            if ct == 'code' and clean_outputs == True:
                if cell.outputs:
                    cell.outputs = []
                    freq['outputs'] += 1
                cell.execution_count = None
        # re-write the json file or return it to a variable
        if save == True:
            with open(nbfile, 'w') as f:
                f.write(json.dumps(nb, indent=2))
        else:
            return json.dumps(nb, indent=2)

    def compare_files(self, existing_file, new_file):
        """Hash and compare two files.

        Returns True if they are equivalent.
        """
        existing_hash = hashlib.sha256(open(existing_file, 'rb').read()).digest()
        new_hash = hashlib.sha256(open(new_file, 'rb').read()).digest()
        if existing_hash == new_hash:
            return True
        else:
            return False

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

    def count_source(self, source):
        """Count number of non-blank lines, words, and non-whitespace characters.

        Used by clean_nb().

        :param source: string to count
        :return: number of non-blank lines, words, and non-whitespace characters
        """
        lines = [line for line in source.split('\n') if line and not line.isspace()]
        words = source.split()
        chars = ''.join(words)
        return len(lines), len(words), len(chars)

    def create_version_dict(self, path=None, version=None):
        """Create and return a version dict.

        If a project path is given, the project folder is zipped
        and compared to the latest existing zip archive. If it
        differs, a new dict is created with a higher version number.
        """
        # Get the latest version number or 1 if it doesn't exist
        if version == None:
            version = self.get_latest_version_number()
        version_dict = self.get_version(version)
        # If a path is given, zip it and compare to the existing hash
        if path is not None:
            # Make sure there is a zipfile to compare
            if 'zipfile' in version_dict and version_dict['zipfile'] is not None:
                # Zip the project path
                new_zipfile = zipfile.ZipFile(path, 'w', zipfile.ZIP_DEFLATED)
                rootlen = len(path) + 1
                for base, _, files in os.walk(path):
                    # Create local paths and write them to the new zipfile
                    for file in files:
                        fn = os.path.join(base, file)
                        new_zipfile.write(fn, fn[rootlen:])
                # Compare the hashes
                result = self.compare_files(version_dict['zipfile'], new_zipfile)
                # If the zipfiles are not the same iterate the version
                if result == False:
                    version = version + 1
                    version_dict['zipfile'] = new_zipfile
        now = datetime.today().strftime('%Y%m%d%H%M%S')
        version_dict['version_name'] = now + '_v' + str(version) + self.name
        version_dict['version_number'] = version
        return version_dict

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

    def save(self, path=None):
        """Handle save requests from the WMS or workspace.

        Default behaviour: Insert a new record.
        """
        # Determine if the project exists in the database
        if self.exists():
            action = 'update'
        else:
            action = 'insert'
        # If a path is supplied, zip it and create a manifest version
        if path is not None:
            self.reduced_manifest['content'] = self.create_version_dict(path)
        # Execute the database query and return the result
        return self.save_record(action)

    def save_record(self, action='insert'):
        """Insert or update a record in the database.
        
        This is a helper function to reduce code repetition.
        """
        try:
            if action == 'update':
                result = projects_db.find_one_and_update({'_id': ObjectId(self._id)},
                                                {'$set': self.reduced_manifest}, upsert=False,
                                                projection={'_id': True}, return_document=ReturnDocument.AFTER)
                _id = result['_id']
            else:
                result = projects_db.insert_one(self.reduced_manifest)
                _id = result.inserted_id
            return {'result': 'success', '_id': _id, 'errors': []}
        except pymongo.errors.OperationFailure as e:
            print(e.code)
            print(e.details)
            return {'result': 'fail', 'errors': ['Error: Could not update the database.']}

    def save_as(self, path=None, new_name=None):
        """Handle save as requests from the WMS or workspace.

        Default behaviour: Insert a new record.
        """
        # Determine if a new name is supplied
        if new_name is None:
            return {'result': 'fail', 'errors': ['No name has been supplied for the new project.']}
        else:
            new_name = datetime.today().strftime('%Y%m%d%H%M%S_') + new_name 
            # If a path is supplied, zip the folder and create a version 1
            if path is not None:
            # Create a new project folder
                try:
                    path_parts = path.split('/')
                    path_parts[-1] = new_name
                    new_path = '/'.join(path_parts)
                    copytree(path, new_path)
                except OSError:
                    return {'result': 'fail', 'errors': ['A project folder with that name already exists. Please try another name.']}
                # Clear Outputs on a glob of all ipynb files
                try:
                    for filename in glob.iglob(path + '/**', recursive=True):
                        if filename.endswith('.ipynb'):
                            self.clean_nb(filename, clean_empty_cells=True, save=True)
                except OSError:
                    # Delete the new directory and fail since we have no way to provide this as a warning.
                    rmtree(new_path)
                    return {'result': 'fail', 'errors': ['Could not clear the notebook variables in the new project folder.']}
                # Change the manifest name and delete the _id
                self.reduced_manifest['name'] = new_name
                if '_id' in self.reduced_manifest:
                    del self.reduced_manifest['_id']
                # If there are any project configs, start files, etc.,
                # they should be reset here.
                # Change the manifest version dict
                self.reduced_manifest['content'] = self.create_version_dict(path, 1)
                # Now insert the record in the database
                result = self.save_record('insert')
                if result is not None:
                    return {'result': 'fail', 'errors': [result]}
                else:
                    return {'result': 'success', '_id': result['_id'], 'errors': []}
            # We just need to insert a new database record with the new name
            else:
                self.reduced_manifest['name'] = new_name
                self.reduced_manifest['content'] = []
                if '_id' in self.reduced_manifest:
                    del self.reduced_manifest['_id']
                result = self.save_record('insert')
                if result is not None:
                    return {'result': 'fail', 'errors': [result]}
                else:
                    return {'result': 'success', '_id': result['id'], 'errors': []}

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
