# IDS-DRR Data Management
Intelligent Data Solution - Disaster Risk Reduction(IDS-DRR) is a system to assist flood management through data-driven ways. The repository contains the code for the data management layer which serves as a bridge between data sources and client applications.

### Import Data
Make migration for layers app: `python manage.py makemigrations`
Run migrations: `python manage.py migrate`
Import data: `python manage.py migrate_data`
Import data for specific state: `python manage.py migrate_data --state assam|HP`
Import data for specific district from a state: `python manage.py migrate_data --state assam --district 201`

## License:
All content in this repository is licensed under
[![GNU-AGPL](https://www.gnu.org/graphics/agplv3-155x51.png)](LICENSE.md)


If you want to contribute, please contact us at info@civicdatalab.in
