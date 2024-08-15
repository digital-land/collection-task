# collection-task

Repository to run the nightly process for a given collection. Produces a docker container which can be used by cloud services to run for any given collection via environment variables. The docker compose shows how to use the image locally.

## Running Locally

This repo allows the team to replicate this task locally to fix any bugs. It doesn't have any of it's own defined python code so testing is limited. The colleciton can be ran using docker or using the task directory and setting up local dependencies to run the code.

### Docker

Given that docker is installed, docker compose can be used to run the process for a given colleciton. Volumes are set up to view outputs but this can cause it to be slow running on some machines. This can be ran using:

```
make compose-up
```

The logs can be viewed via docker desktop or similar tools. Once it has ran you can run 

```
make compose-down
```

to remove the container and the images you created.

You can edit the collection by changing the environment variable in the docker compose file.

### Without Docker

There is a way to run the process without docker you will need to set up some dependencies:
- install GNU make
- set up a virtual environment with python (3.8 or 3.9)

Once the above is done move into the task directory
```
cd task
```

set the  name of the collection you want to process to an Environment variables
```
export COLLECTION_NAME=ancient-woodland
```

From here you can either use the make rules to run run the collector (and/or different stages) e.g.

```
make init
make -j 10
```

or run the bash script that's used in the docker image. (if you haven't added a COLLECTION_DATASET_BUCKET_NAME then it will just skip the AWS steps)

```
./run.sh
```

Equally make rules havebeen set up in the root of the repository which allow you to run  the ./run.sh without changing to the task folder. e.g.

```
make run COLLECTION_NAME=<insert_collection_name_here>
```

you can include other environment variables to control the task

```
make run COLLECTION_NAME=<insert_collection_name_here> TRANSFORMED_JOBS=8
```
make clean and make clobber have also been set up to clear the task folder