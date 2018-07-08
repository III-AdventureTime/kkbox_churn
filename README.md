1. 'Pull' (or 'clone', for the first time) this repository to your local working directory

2. Create task folder (needed only for a new task):
```bash
$ cd kkbox_churn/
$ cp -r template.yang your_task_name.your_name
``` 

3. Enter into your task folder and start editing your codes. 
    + *Do not edit any files in other member's folders.*
    + The output of your program should be stored in
        - `hdfs://master:8020/kkbox_churn/data01/<XXX[.label]>/`
        - or, in local file system of *master* (10.120.27.103): `/home/master/iii_projects_data/kkbox_churn/data01/<XXX[.label]>/`
        - examples of `<XXX[.label]>/`
            * 001/
            * 002.clean_members/

4. Describe your codes (in the beginning of code file, or in `description.md`)
    + where and what the input data is
    + what your codes do
    + output data
        - data location
        - brief description
        - detailed description

5. When ready to 'push' to GitHub:
    1. edit `.gitignore` (in task folder) when necessary
    2. in folder `kkbox_churn/`, do the following:
```bash
$ git add --all
$ git commit -m "describe your updates"
$ git pull   # because maybe someone has updated his/her files
$ git push
```
