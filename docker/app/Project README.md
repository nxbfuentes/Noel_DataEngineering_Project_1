# Project 1

## Context

Over the past 4 weeks, you have learnt:

- How to build robust ETL pipelines using Python, Pandas, SQL and SQLAlchemy
- How to use YAML files to configure what your ETL pipeline does (metadata configuration)
- How to use Jinja2 with SQL to templatise your SQL files with macros and variables
- How to log your ETL pipeline results to a database table
- How to perform full and incremental extracts
- How to perform full, incremental and upsert loads
- How to create a docker image using a Dockerfile
- How to use git to commit code and collaborate with teams using branches
- How to package and host an ETL solution on Amazon Web Services (AWS) using services such as ECS, ECR, S3, RDS and IAM

## Goal

- Work in a team to create an ETL pipeline using patterns and concepts covered previously.
- Your ETL pipeline should serve the goal of providing useful information to end users such as a data analyst or data scientist.
- You may choose any dataset(s) that you and your team would like to work with. For example, you may choose a dataset based on your team's personal or professional interests, or based on the availability and accessibility to data.
- Your ETL pipeline should have tests and logging.
- Deploy and schedule your ETL pipeline on the cloud.
- Present and demo your working solution to the class.

Here are some data sources (this is not an extensive list and you are encouraged to do your own research):

|Data source name|URL|
|--|--|
|Public APIs|https://github.com/public-apis/public-apis|
|Australian Government Open Source Datasets| https://data.gov.au/|
|Kaggle Open Source Datasets|https://www.kaggle.com/datasets|
|Australian Bureau of Statistics |https://www.abs.gov.au/|
|World Bank Open Data|https://data.worldbank.org/|
|Google dataset search|https://toolbox.google.com/datasetsearch|
|Sample Postgres databases|https://www.postgresql.org/ftp/projects/pgFoundry/dbsamples/|

## Timeline

**Total duration: 2 weeks**

- **Day 1**: Provide a project plan (refer to [project-plan-template.md](project-plan-template.md))
- **Day 2 - Day 6**: Work on your project in class (and outside of class)
- **Day 7**: Project submission
- **Day 7**: 8 minute presentation of your project

## Project submission

:arrow_down: Please submit your GitHub repository link to the Project Submission Form below. Please ensure that your GitHub repository is Public so that we are able to access it.

[![Project Submission Form](../admin/submit-feedback.png)](https://forms.gle/Ueph3b4iaEVHRFPK9)

## Learning objectives

By the end of this project, you will have hopefully learnt the following:

1. Working in teams using Git (git commits, push, branching and pull requests)
1. Dividing work effectively between team members in data engineering projects
1. Apply full or incremental extraction techniques to your data source(s)
1. Apply full or incremental or upsert loading techniques to your target database or data lake
1. Apply data transformation and enrichment techniques to your data
1. Apply docker containerisation techniques to build a docker image of your ELT/ETL solution
1. Apply logging techniques to enable easy tracking of the pipeline status
1. Apply unit tests to ETL/ELT steps
1. Deploy the ETL/ELT solution to the cloud

## Requirements and rubric

<table>
    <tr>
        <th>Requirement</th>
        <th>Percentage of marks</th>
    </tr>
    <tr>
        <td>
            Extract data from either a static or live dataset.
            <li>A static dataset refers to a dataset that is not changing e.g. a CSV file.  </li>
            <li>A live dataset refers to a dataset that has data updating periodically (e.g. every day, every hour, every minute).</li>
            <li>A live dataset can also refer to a dataset that you have full control over (e.g. a database table) and can manually insert data into the database table to mimic a live dataset.</li>
        </td>
        <td>
            <li>Static dataset: 5%</li>
            <li>Live dataset: 10%</li>
        </td>
    </tr>
    <tr>
        <td>
            Extract data using either full extract or incremental extract.
            <li>A full extract refers to a full read of a file, a full read of a database table, or a full read from an API endpoint. </li>
            <li>An incremental extract refers to reading a database table with a filter condition on a timestamp column e.g. `where event_date > '2020-01-01'`, or reading from an API endpoint with a query parameter e.g. `localhost/api?date=2020-01-01`. Incremental values would need to be persisted in a file or database table in order for the incremental logic to work for subsequent runs.</li>
        </td>
        <td>
            <li>Full extract: 5%</li>
            <li>Incremental extract: 10%</li>
        </td>
    </tr>
    <tr>
        <td>
            Load data to either a database table or file using either full load, incremental load, or upsert load.
            <li>A full load refers to overwriting all records in the target table or file with new records. </li>
            <li>An incremental load refers to inserting only new records to the target table or file.</li>
            <li>An upsert load refers to inserting new records or updating existing records to the target table or file.</li>
        </td>
        <td>
            <li>Full load: 5%</li>
            <li>Incremental load: 7%</li>
            <li>Upsert load: 10%</li>
        </td>
    </tr>
    <tr>
        <td>
            Transform data using the following techniques:
            <li>Aggregation function e.g. `avg`, `sum`, `max`, `min`, `count`, `rank`</li>
            <li>Grouping i.e. `group by`</li>
            <li>Window function e.g. `partition by`</li>
            <li>Calculation e.g. `column_A + column_B`</li>
            <li>Data type casting</li>
            <li>Filtering e.g. `where`, `having`</li>
            <li>Sorting</li>
            <li>Joins/merges</li>
            <li>Unions</li>
            <li>Renaming e.g. `df.rename(columns={"old":"new:})` or `columnA as column_A` </li>
        </td>
        <td>
            <li>3 transformation techniques: 5%</li>
            <li>5 transformation techniques: 7%</li>
            <li>7 transformation techniques: 10%</li>
        </td>
    </tr>
    <tr>
        <td>
            Using git for collaboration:
            <li>Git commits and git push</li>
            <li>Git branching</li>
            <li>Pull request and review</li>
        </td>
        <td>
            <li>Git commits and push only: 5%</li>
            <li>+ Git branching: 7%</li>
            <li>+ Pull request and review: 10%</li>
        </td>
    </tr>
    <tr>
        <td>
            Implement unit tests using PyTest or similar
        </td>
        <td>
            <li>1 unit test: 2.5% </li>
            <li>2 unit tests: 5% </li>
        </td>
    </tr>
    <tr>
        <td>
            Write pipeline metadata logs to a database table
        </td>
        <td>
            Pipeline metadata logging: 5%
        </td>
    </tr>
    <tr>
        <td>
            Build a docker image using a Dockerfile (the teaching staff will be verifying that the docker image builds locally)
        </td>
        <td>
            Docker image: 5%
        </td>
    </tr>
    <tr>
        <td>
            Docker container runs locally (the teaching staff will be verifying that the docker image runs locally)
        </td>
        <td>
            Running Docker container: 5%
        </td>
    </tr>
    <tr>
        <td>
            Deploy docker container to Amazon Web Services (provide screenshot evidence of services configured/running):
            <li>Elastic Container Service (ECS) - screenshot of scheduled task in ECS</li>
            <li>Elastic Container Registry (ECR) - screenshot of image in ECR</li>
            <li>Relational Database Service (RDS) or Simple Storage Service (S3) depending on your choice of target storage - screenshot of dataset in target storage</li>
            <li>IAM Role - screenshot of created role</li>
            <li>S3 for `.env` file - screenshot of `.env` file in S3</li>
        </td>
        <td>
            <li>ECS, ECR and your choice of target storage (RDS or S3): 10%</li>
            <li>+ IAM Role and S3 for `.env` file: 15%</li>
        </td>
    </tr>
    <tr>
        <td>
            Presentation - explain the following:
            <li>Project context and goals</li>
            <li>Datasets selected</li>
            <li>Solution architecture diagram using <a href="https://www.draw.io/">draw.io</a> or similar. See <a href="https://about.gitlab.com/handbook/business-technology/data-team/platform/#our-data-stack">GitLab's data platform architecture diagram</a> as an example.</li>
            <li>ELT/ETL techniques applied</li>
            <li>Final dataset and demo run (if possible)</li>
            <li>Lessons learnt</li>
        </td>
        <td>
            <li>Project context and goals: 1%</li>
            <li>Datasets selected: 1%</li>
            <li>Solution architecture diagram: 2%</li>
            <li>ELT/ETL techniques applied: 2%</li>
            <li>Final dataset and demo run (if possible): 2%</li>
            <li>Lessons learnt: 2%</li>
        </td>
    </tr>
    <tr>
        <td>
            Documentation
            <li>Code documentation using <a href="https://realpython.com/documenting-python-code/#documenting-your-python-code-base-using-docstrings">Python docstrings and comments</a> where reasonable</li>
            <li>Markdown documentation explaining the project context, architecture and installation/running instructions. See <a href="https://github.com/matiassingers/awesome-readme">here</a> for examples.</li>
        </td>
        <td>
            <li>Code documentation using Python docstrings and comments where reasonable: 3%</li>
            <li>Markdown documentation explaining the project context, architecture and installation/running instructions: 4%</li>
        </td>
    </tr>
</table>

## Tips

- **Divide and conquer**: Find ways to parallelise the work you do as a team. For example, assuming an EL/T pattern:
  - Step 1 (In parallel):
    - Person A and B pair program on the Extract and Load pipeline
    - Person C and D pair program on the Transform pipeline
  - Step 2 (In parallel):
    - Person A and C pair program on stitching the ELT pipeline together, adding logging and creating the Dockerfile for the docker image
    - Person B and D pair program on creating the required AWS services (e.g. RDS, ECR, S3, ECS)
  - Step 3 (In parallel):
    - Person A and B pair program on writing unit tests, documentation, and preparing slides for the presentation
    - Person C and D pair program on deploying the solution to AWS
- **Don't overthink it**: We're not looking for the perfect solution with every minor detail resolved. It is okay to incur [technical debt](https://www.productplan.com/glossary/technical-debt/) to get to the end goal quickly for the project due to time constraints. In the real world, we would come back later to pay down the technical debt we've incurred by fixing the loose ends.
- **Stick to the requirements and rubric**: We will be assessing your project based on the requirements in the rubric. Aim to tick off items in the rubric before looking to go beyond the scope.
- **Give it a good go, but know when to ask for help**: Always have a good go before asking for help as that is the best way you will exercise your problem solving muscles. However, if you find yourself spending more than 20-30 minutes on a single challenging problem, with no clear idea of how you will solve it, then reach out to your teammates or the teaching staff for help.
