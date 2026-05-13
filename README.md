# collection-task

Repository to run the nightly process for a given collection. Produces a docker container which can be used by cloud services to run for any given collection via environment variables. The docker compose shows how to use the image locally.

## Running collection-task locally

This repo allows the team to replicate this task locally to fix any bugs. It doesn't have any of it's own defined python code so testing is limited. The colleciton can be ran using docker or using the task directory and setting up local dependencies to run the code.

### Run locally in a virtual environment - for the first time

There is a way to run the process without docker you will need to [set up some dependencies](https://digital-land.github.io/technical-documentation/development/tutorials/set-up-for-mac/):
- install GNU make
- set up a virtual environment with python (3.8 or 3.9)

Ensure you are inside the virtual environment is activated
```
source .venv/bin/activate
```

Set the  name of the collection you want to process to an Environment variables
```
export COLLECTION_NAME=ancient-woodland
```

Install requirements and downloads specification files and config files

```
make init
```

You can enable Make to run multiple jobs at once (this can speed up running the transform on large datasets)
```
make -j 10
```

If you have made any code changes in a sister (repo in same folder as collection-task) respository e.g. digital-land-python, you will need to create a pointer so collection-task uses this local repo over the GitHub version. You can do that by running the following:
```
make dev
```

You can now run the whole pipeline with the following: 
```
make run COLLECTION_NAME=<insert_collection_name_here> TRANSFORMED_JOBS=8
```

:memo: **Note:** TRANSFORMED_JOBS=8 is an optional extra to speed up the transform step on large datasets. This uses bin/run.sh which exists only for running collect-task locally.

Alternatively you can run each of the four sections of a collect-task separately:

```
make collect
make collection
make transformed
gmake dataset   # note: gmake not make for this step
```

:memo: **Note:** gmake is required if you have not followed the [setup-for-mac](https://digital-land.github.io/technical-documentation/development/tutorials/set-up-for-mac/) instructions on your machine.



### Run locally in a virtual environment - all subsequent runs.

#### Tidy up data from previous runs:

Option 1: 
```
make clobber
````
This will remove the pipeline output directories the files and folders generated during the transform/dataset steps, but will keep the downloaded collection data.

:memo: **Note:** You would use this when you want to re-run `make transformed` and `gmake dataset`

Option 2:
```
make Clean
```
This does everything `make clobber` does but also removes logs, resouces csvs, state.json, downloaded pipeline config csvs and collection config csvs.
```
:memo: **Note:** You would do this when you want to re-run a pipeline totally from scratch.


#### Running the pipeline again

Choose whichever bit of the pipeline you want to run again.
```
make collect
make collection
make transformed
gmake dataset   # note: gmake not make for this step
```

### How to swap between fetching code from local repositories and from github


If you have made some changes to local sister repositories you can run: 
```
make dev
```
If you want to swap back to using code from github 
```
make init
```
Whichever runs last wins. So:
* `make init` after `make dev` → back to GitHub
* `make dev` after `make init` → back to your local repo



### Docker
:warning: **Warning:** This currently errors but a [tech-debt ticket](https://github.com/orgs/digital-land/projects/15/views/3?pane=issue&itemId=186630905&issue=digital-land%7Ccollection-task%7C53) has been made to look into fixing it.

Given that docker is installed, docker compose can be used to run the process for a given collection. Volumes are set up to view outputs but this can cause it to be slow running on some machines. This can be ran using:

```
make compose-up
```

The logs can be viewed via docker desktop or similar tools. Once it has ran you can run 

```
make compose-down
```

to remove the container and the images you created.

You can edit the collection by changing the environment variable in the docker compose file.

