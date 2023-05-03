import logging
import os
import sys
import time
from datetime import datetime

import planon
import ipaas.utils

# *********************************************************************
# SETUP
# *********************************************************************

log_level = os.environ.get("LOG_LEVEL", "INFO")
log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

logging.basicConfig(stream=sys.stdout, level=log_level, format=log_format)
# logging.basicConfig(filename='logging.log', level = log_level, format = log_format)

# Set the log to use GMT time zone
logging.Formatter.converter = time.gmtime

# Add milliseconds
logging.Formatter.default_msec_format = "%s.%03d"

log = logging.getLogger(__name__)

# *********************
# PLANON
# *********************

planon.PlanonResource.set_site(site=os.environ["PLANON_API_URL"])
planon.PlanonResource.set_header(jwt=os.environ["PLANON_API_KEY"])

# *********************************************************************
# MAIN
# *********************************************************************

start = datetime.utcnow()

# ********************************************************************
# Source DARTMOUTH Billing accounts
# Loop through all chart of accounts based on the segment type in iPaas
# *********************************************************************

log.info("Getting chart of accounts segments from Dartmouth")

dartmouth_entities = {entity["entity"]: entity for entity in ipaas.utils.get_coa_segment(segment="entities")}
dartmouth_orgs = {org["org"]: org for org in ipaas.utils.get_coa_segment(segment="orgs")}
dartmouth_fundings = {funding["funding"]: funding for funding in ipaas.utils.get_coa_segment(segment="fundings")}
dartmouth_activities = {activity["activity"]: activity for activity in ipaas.utils.get_coa_segment(segment="activities")}
dartmouth_subactivities = {
    (subactivity["subactivity"], subactivity["subactivity_description"]): subactivity for subactivity in ipaas.utils.get_coa_segment(segment="subactivities")
}
dartmouth_natural_classes = {natural_class["natural_class"]: natural_class for natural_class in ipaas.utils.get_coa_segment(segment="natural_classes")}

# ***********************************************************************
# Source PLANON Billing accounts
# Loop through all chart of accounts in Planon
# ***********************************************************************

log.info("Getting chart of accounts segments from Planon")

segments_filter = {
    "filter": {
        "FreeString11": {"exists": True},  # Segment type
    }
}

planon_coa_segments = planon.UsrBillingAccounts.find(segments_filter)

planon_entities = {entity.Code: entity for entity in planon_coa_segments if entity.SegmentType == "SEG1"}
planon_orgs = {org.Code: org for org in planon_coa_segments if org.SegmentType == "SEG2"}
planon_fundings = {funding.Code: funding for funding in planon_coa_segments if funding.SegmentType == "SEG3"}
planon_activities = {activity.Code: activity for activity in planon_coa_segments if activity.SegmentType == "SEG4"}
planon_subactivities = {(subactivity.Code, subactivity.Name): subactivity for subactivity in planon_coa_segments if subactivity.SegmentType == "SEG5"}
planon_natural_classes = {natural_class.Code: natural_class for natural_class in planon_coa_segments if natural_class.SegmentType == "SEG6"}

log.info(
    f"From Dartmouth retrieved {len(dartmouth_entities)} entities, {len(dartmouth_orgs)} orgs, {len(dartmouth_fundings)} fundings, {len(dartmouth_activities)} activities, {len(dartmouth_subactivities)} subactivities, {len(dartmouth_natural_classes)} natural classes"
)

log.info(
    f"From Planon retrieved {len(planon_entities)} entities, {len(planon_orgs)} orgs, {len(planon_fundings)} fundings, {len(planon_activities)} activities, {len(planon_subactivities)} subactivities, {len(planon_natural_classes)} natural classes"
)

succeeded = []
failed = []
archived = []

# *********************
# ENTITIES
# *********************

log.info("# **************** Processing COA entities **************** #")

# ********************* INSERTS ********************* #

# Inserts new entity in Planon, if it doesn't exist
inserts = set(dartmouth_entities) - set(planon_entities)
log.info(f"Total number of entities to be inserted in Planon {len(inserts)}")

for insert in inserts:
    log.info(f"Processing insert {insert}")

    try:
        dartmouth_entity = dartmouth_entities[insert]  # dict comprehension to match both sides

        planon_entity = planon.UsrBillingAccounts.create(
            values={
                "Code": dartmouth_entity["entity"],
                "Name": dartmouth_entity["entity_description"],
                "FreeString11": "SEG1",  # Segment type
            }
        )

        log.info(f"Successfully added {insert}")
        succeeded.append(insert)

    except Exception as e:
        log.exception(e)
        failed.append(insert)

# ********************* UPDATES ********************* #

# Updates name in Planon side, if there is a change:
updates = set(dartmouth_entities).intersection(set(planon_entities))
log.info(f"Total number of entities to be updated in Planon {len(updates)}")

for update in updates:
    try:
        dartmouth_entity = dartmouth_entities[update]
        planon_entity = planon_entities[update]

        if dartmouth_entity["entity_description"] != planon_entity.Name:
            log.info(f"Processing update {update}")
            planon_entity.Name = dartmouth_entity["entity_description"]
            planon_entity = planon_entity.save()

            log.info(f"Successfully updated {planon_entity.Name} with {planon_entity.Code} ")
            succeeded.append(planon_entity)

    except Exception as e:
        log.exception(e)
        failed.append(update)

# ********************* ARCHIVES ********************* #

# TODO - Do we need to archive entities that are not in Dartmouth anymore?
# Archives entities in Planon, if it doesn't exist in Dartmouth
archives = set(planon_entities) - set(dartmouth_entities)
log.info(f"Total number of subactivities to be archived in Planon {len(archives)}")

for archive in archives:
    log.info(f"Processing archive {archive}")

    try:
        planon_entity = planon_entities[archive]

        if planon_entity.IsArchived == False:
            log.info(f"Archiving {planon_entity.Name} with {planon_entity.Code} ")
            planon_entity.execute(bom="BomArchive")

            log.info(f"Successfully archived {planon_entity.Name} with {planon_entity.Code} ")
            archived.append(planon_entity)

    except Exception as e:
        log.exception(e)
        failed.append(archive)

# *********************
# ORGS
# *********************

log.info("# **************** Processing COA orgs **************** #")

# ********************* INSERTS ********************* #

# Inserts new org in Planon, if it doesn't exist
inserts = set(dartmouth_orgs) - set(planon_orgs)
log.info(f"Total number of orgs to be inserted in Planon {len(inserts)}")

for insert in inserts:
    log.info(f"Processing insert {insert}")

    try:
        dartmouth_org = dartmouth_orgs[insert]  # dict comprehension to match both sides

        planon_org = planon.UsrBillingAccounts.create(
            values={
                "Code": dartmouth_org["org"],
                "Name": dartmouth_org["org_description"],
                "FreeString11": "SEG2",  # Segment type
            }
        )

        log.info(f"Successfully added {insert}")
        succeeded.append(insert)

    except Exception as e:
        log.exception(e)
        failed.append(insert)

# ********************* UPDATES ********************* #

# Updates
updates = set(dartmouth_orgs).intersection(set(planon_orgs))
log.info(f"Total number of orgs to be updated in Planon {len(updates)}")

for update in updates:
    try:
        dartmouth_org = dartmouth_orgs[update]
        planon_org = planon_orgs[update]

        if dartmouth_org["org_description"] != planon_org.Name:
            log.info(f"Processing update {update}")
            planon_org.Name = dartmouth_org["org_description"]
            planon_org = planon_org.save()

            log.info(f"Successfully updated {planon_org.Name} with {planon_org.Code} ")
            succeeded.append(planon_org)

    except Exception as e:
        log.exception(e)
        failed.append(update)

# ********************* ARCHIVES ********************* #

# TODO - Do we need to archive orgs that are not in Dartmouth anymore?
# Archives orgs in Planon, if it doesn't exist in Dartmouth
archives = set(planon_orgs) - set(dartmouth_orgs)
log.info(f"Total number of orgs to be archived in Planon {len(archives)}")

for archive in archives:
    log.info(f"Processing archive {archive}")

    try:
        planon_org = planon_orgs[archive]

        if planon_org.IsArchived == False:
            log.info(f"Archiving {planon_org.Name} with {planon_org.Code} ")
            planon_org.execute(bom="BomArchive")

            log.info(f"Successfully archived {planon_org.Name} with {planon_org.Code} ")
            archived.append(planon_org)

    except Exception as e:
        log.exception(e)
        failed.append(archive)

# *********************
# FUNDINGS
# *********************

log.info("# **************** Processing COA fundings **************** #")

# ********************* INSERTS ********************* #

# Inserts:
inserts = set(dartmouth_fundings) - set(planon_fundings)
log.info(f"Total number of fundings to be inserted in Planon {len(inserts)}")

for insert in inserts:
    log.info(f"Processing insert {insert}")

    try:
        dartmouth_funding = dartmouth_fundings[insert]

        planon_funding = planon.UsrBillingAccounts.create(
            values={
                "Code": dartmouth_funding["funding"],
                "Name": dartmouth_funding["funding_description"],
                "FreeString11": "SEG3",  # Segment type
            }
        )

        log.info(f"Successfully added {insert}")
        succeeded.append(insert)

    except Exception as e:
        log.exception(e)
        failed.append(insert)

# ********************* UPDATES ********************* #

# Updates:
updates = set(dartmouth_fundings).intersection(set(planon_fundings))
log.info(f"Total number of fundings to be updated in Planon {len(updates)}")

for update in updates:
    try:
        dartmouth_funding = dartmouth_fundings[update]
        planon_funding = planon_fundings[update]

        if dartmouth_funding["funding_description"] != planon_funding.Name:
            log.info(f"Processing update {update}")
            planon_funding.Name = dartmouth_funding["funding_description"]
            planon_funding = planon_funding.save()

            log.info(f"Successfully updated {planon_funding.Name} with {planon_funding.Code} ")
            succeeded.append(planon_funding)

    except Exception as e:
        log.exception(e)
        failed.append(update)

# ********************* ARCHIVES ********************* #

# TODO - Do we need to archive fundings that are not in Dartmouth anymore?
# Archives orgs in Planon, if it doesn't exist in Dartmouth
archives = set(planon_fundings) - set(dartmouth_fundings)
log.info(f"Total number of fundings to be archived in Planon {len(archives)}")

for archive in archives:
    log.info(f"Processing archive {archive}")

    try:
        planon_funding = planon_fundings[archive]

        if planon_funding.IsArchived == False:
            log.info(f"Archiving {planon_funding.Name} with {planon_funding.Code} ")
            planon_funding.execute(bom="BomArchive")

            log.info(f"Successfully archived {planon_funding.Name} with {planon_funding.Code} ")
            archived.append(planon_funding)

    except Exception as e:
        log.exception(e)
        failed.append(archive)

# *********************
# ACTIVITIES
# *********************

log.info("# **************** Processing COA activities **************** #")

# ********************* INSERTS ********************* #

# Inserts:
inserts = set(dartmouth_activities) - set(planon_activities)
log.info(f"Total number of activities to be inserted in Planon {len(inserts)}")

for insert in inserts:
    log.info(f"Processing insert {insert}")

    try:
        dartmouth_activity = dartmouth_activities[insert]

        planon_activity = planon.UsrBillingAccounts.create(
            values={"Code": dartmouth_activity["activity"], "Name": dartmouth_activity["activity_description"], "FreeString11": "SEG4"}  # Segment type
        )

        log.info(f"Successfully added {insert}")
        succeeded.append(insert)

    except Exception as e:
        log.exception(e)
        failed.append(insert)

# ********************* UPDATES ********************* #

# Updates:
updates = set(dartmouth_activities).intersection(set(planon_activities))
log.info(f"Total number of activities to be updated in Planon {len(updates)}")

for update in updates:
    try:
        dartmouth_activity = dartmouth_activities[update]
        planon_activity = planon_activities[update]

        if dartmouth_activity["activity_description"] != planon_activity.Name:
            log.info(f"Processing update {update}")
            planon_activity.Name = dartmouth_activity["activity_description"]
            planon_activity = planon_activity.save()

            log.info(f"Successfully updated {planon_activity.Name} with {planon_activity.Code} ")
            succeeded.append(planon_activity)

    except Exception as e:
        log.exception(e)
        failed.append(update)

# ********************* ARCHIVES ********************* #

# TODO - Do we need to archive activities that are not in Dartmouth anymore?
# Archives orgs in Planon, if it doesn't exist in Dartmouth
archives = set(planon_activities) - set(dartmouth_activities)
log.info(f"Total number of activities to be archived in Planon {len(archives)}")

for archive in archives:
    log.info(f"Processing archive {archive}")

    try:
        planon_activity = planon_activities[archive]

        if planon_activity.IsArchived == False:
            log.info(f"Archiving {planon_activity.Name} with {planon_activity.Code} ")
            planon_activity.execute(bom="BomArchive")

            log.info(f"Successfully archived {planon_activity.Name} with {planon_activity.Code} ")
            archived.append(planon_activity)

    except Exception as e:
        log.exception(e)
        failed.append(archive)

# *********************
# SUBACTIVITIES
# *********************

# Subactivities are handled differently than the other segments because they are not a 1:1 mapping between Dartmouth and Planon. 
# Unlike the other segments the subactivity is a child of the activity and the codes are not unique. 
# The subactivity is unique within a given activity.
# We do not want to show multiple subactivities of the same code in the UI, instead we will ensure that there is at least one subactivity for a given code + description.
# If a code + description is no longer present in Dartmouth we will archive the subactivity in Planon.

log.info("# **************** Processing COA subactivities **************** #")

log.info(f"Total number of Planon subactivities {len(planon_subactivities)}")

# ********************* INSERTS ********************* #

# Inserts:
inserts = set(dartmouth_subactivities) - set(planon_subactivities)
log.info(f"Total number of subactivities to be inserted in Planon {len(inserts)}")

for insert in inserts:
    log.info(f"Processing insert {insert}")
    if insert:
        try:
            dartmouth_subactivity = dartmouth_subactivities[insert]
            planon_subactivity = planon.UsrBillingAccounts.create(
                values={
                    "Code": dartmouth_subactivity["subactivity"],
                    "Name": dartmouth_subactivity["subactivity_description"],
                    "FreeString11": "SEG5",  # Segment type
                }
            )
            log.info(f"Successfully added {insert}")
            succeeded.append(insert)

        except Exception as e:
            log.exception(e)
            log.info(f"Failed to add {insert}")

# ********************* ARCHIVES ********************* #

# Archives subactivities in Planon, if it doesn't exist in Dartmouth
archives = set(planon_subactivities) - set(dartmouth_subactivities)
log.info(f"Total number of subactivities to be archived in Planon {len(archives)}")

for archive in archives:
    log.info(f"Processing archive {archive}")

    try:
        planon_subactivity = planon_subactivities[archive]

        if planon_subactivity.IsArchived == False:
            log.info(f"Archiving {planon_subactivity.Name} with {planon_subactivity.Code} ")
            planon_subactivity.execute(bom="BomArchive")

            log.info(f"Successfully archived {planon_subactivity.Name} with {planon_subactivity.Code} ")
            archived.append(planon_subactivity)

    except Exception as e:
        log.exception(e)
        failed.append(archive)

# *********************
# NATURAL_CLASSES
# *********************

log.info("# **************** Processing COA natural classes **************** #")

# ********************* INSERTS ********************* #

# Inserts:
inserts = set(dartmouth_natural_classes) - set(planon_natural_classes)
log.info(f"Total number of natural classes to be inserted in Planon {len(inserts)}")

for insert in inserts:
    log.info(f"Processing insert {insert}")

    try:
        dartmouth_natural_class = dartmouth_natural_classes[insert]

        planon_natural_class = planon.UsrBillingAccounts.create(
            values={
                "Code": dartmouth_natural_class["natural_class"],
                "Name": dartmouth_natural_class["natural_class_description"],
                "FreeString11": "SEG6",  # Segment type
            }
        )

        log.info(f"Successfully added {insert}")
        succeeded.append(insert)

    except Exception as e:
        log.exception(e)
        failed.append(insert)

# ********************* UPDATES ********************* #

# Updates
updates = set(dartmouth_natural_classes).intersection(set(planon_natural_classes))
log.info(f"Total number of natural classes to be updated in Planon {len(updates)}")

for update in updates:
    try:
        dartmouth_natural_class = dartmouth_natural_classes[update]
        planon_natural_class = planon_natural_classes[update]

        if dartmouth_natural_class["natural_class_description"] != planon_natural_class.Name:
            log.info(f"Processing update {update}")
            planon_natural_class.Name = dartmouth_natural_class["natural_class_description"]
            planon_natural_class = planon_natural_class.save()

            log.info(f"Successfully updated {planon_natural_class.Name} with {planon_natural_class.Code} ")
            succeeded.append(planon_natural_class)

    except Exception as e:
        log.exception(e)
        failed.append(update)

# ********************* ARCHIVES ********************* #

# TODO - Do we need to archive natural_classes that are not in Dartmouth anymore?
# Archives natural_classes in Planon, if it doesn't exist in Dartmouth
archives = set(planon_natural_classes) - set(dartmouth_natural_classes)
log.info(f"Total number of natural_classes to be archived in Planon {len(archives)}")

for archive in archives:
    log.info(f"Processing archive {archive}")

    try:
        planon_natural_class = planon_natural_classes[archive]

        if planon_natural_class.IsArchived == False:
            log.info(f"Archiving {planon_natural_class.Name} with {planon_natural_class.Code} ")
            planon_natural_class.execute(bom="BomArchive")

            log.info(f"Successfully archived {planon_natural_class.Name} with {planon_natural_class.Code} ")
            archived.append(planon_natural_class)

    except Exception as e:
        log.exception(e)
        failed.append(archive)

