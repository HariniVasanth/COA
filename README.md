# COA
# feed_Chart_of_Accounts
This Python automation script will feed the chart of accounts based of segment typ  from GL using iPaas and feed it to Planon under Base chart of accounts 

## Getting started
```bash:
python -m venv venv
source venv/bin/activate
add source .env to venv/bin/activate (to execute in python shell)
pip install wheel
pip install git+ssh://git@git.dartmouth.edu/planon/libraries/libplanon-rest.git@ *Copied Commit SHA from gitlab*(manually install planon )
pip install -r requirements.txt

OR

Use dev containers
 ```

## Add your files
```
cd existing_repo
git remote add origin 
git branch -M master
git push -uf origin master
```
## Setup:
source .env or using dev container setup env



## Description:

url = f"{base_url}/api/general_ledger/{segment}?parent_child_flag=C" # for all other segments , except subactivity
~~~
if segment in ['entities', 'orgs', 'fundings', 'activities', 'natural_classes']:
        url = f"{url}?parent_child_flag=C"
~~~

Subactivity is different, as all subactivities are always 'C' meaning Child to the parent activity
        ***segs***=entities
        ***seg***=entity

        #resource request for ipaas:
        dartmouth_***segs*** = {
                ***seg***["***seg***"]: ***seg*** for ***seg*** in ipaas.utils.get_coa_segment
                (
                jwt=dart_jwt, 
                base_url=DARTMOUTH_API_URL,
                segment="***segs***",
                session=session
                )
                }
        
        inserts = set(dartmouth_***segs***.keys()) - set(planon_***segs***.keys())

        for insert in inserts: 
                dartmouth_***seg*** = dartmouth_**segs**[insert]

        planon_***seg*** = planon.UsrBillingAccounts.create(
            values={
                "Code": dartmouth_***seg***["***seg***"],
                "Name": dartmouth_***seg***["***seg***_description"],
                "FreeString11": segment
            }
        )
