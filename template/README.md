# README

This folder contains the template files for a WE1S project. The structure and use of the templates will need to be fully documented, but here are some preliminary notes:

- The file `datapackage.json` is a sample file. 
- The file `start_template.ipynb` contains the clean html for the Start page, whereas the file `start.ipynb` is re-written when modules are run. When a project is deployed live, any previous `start.ipynb` should be deleted and the `start_template.ipynb` file should be renamed to `start.ipynb`.

Some further details:

## The `datapackage.json` File

This file is a Frictionless Data datapackage manifest adapted to the WE1S manifest schema. The WE1S schema requires the following properties: `name`, `namespace`, `metapath` (always "Projects"), `title`, `contributors`, `db-query`. Additionally, it may have an `_id` property assigned by MongoDB, as well as other properties described in the [manifest schema documentation](https://github.com/whatevery1says/manifest/blob/master/we1s-manifest-schema-2.0.md#we1s-projects), and it may also include _ad hoc_ properties as needed. Frictionless Data requires the `resources` property. In the WE1S manifest schema, this is a list of paths to `Sources`, `Corpus`, `Processes`, and `Scripts`. In light of current re-factoring efforts, this need not be adhered to strictly. Instead, a list of paths to template modules and their assets should be adopted.

## The Start Page Notebook

The `start.ipynb` notebook consists (currently) of a single cell containing pre-rendered html to serve as a UI starting point for each project. This provides convenient links to the project's modules and other assets. As the processes are run within the project, the `start.ipynb` file is re-written, and refreshing the page or cell will reflect the new state.

Each UI element in the cell's html has an id, the value of which may be updated programmatically at the end of each project notebook or by helper scripts. A log of the most recent state of the project is stored in (`start.json`), which supplies the initial values written to the start page. Eventually, this configuration might be moved to the datapackage file.

The Start Page should have the following elements:

1. The name of the project
2. A url to the project's file tree folder.
3. A list of available modules and their paths.
4. A list of available modules and project-level operations. If these produced outputs like visualisations, the urls to these outputs should be stored.

The above requirements suggest the following structure:

```json
{
    "name": "my-project-name",
    "project_url": "http:\/\/harbor.english.ucsb.edu:10000\/tree\/write\/dev/20190224_1906_reddit-jokes",
    "operations": [
        "re-import",
        "save",
        "save-as"
    ],
    "available_modules": [
        "topic-modeling",
        "dfr-browser",
        "pyldavis",
        {
          "interpretation-protocols": [
            "module1",
            "module2"
          ]
        }
    ],
    "visualisations": [
        {
            "dfr-browser": "http:\/\/harbor.english.ucsb.edu:10001\/dev\/20190224_1906_reddit-jokes\/browser\/",
            "pyldavis": "http:\/\/harbor.english.ucsb.edu:10000\/view\/write\/dev/20190224_1906_reddit-jokespyldavis\/index.html"
        }
    ]
}
```