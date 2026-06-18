# IDS-DRR Data Management

Intelligent Data Solution for Disaster Risk Reduction (IDS-DRR) is a platform that assists disaster management decision-making through data-driven analytics. This repository contains the **data management layer**: the backend that ingests geographic and indicator data and serves them via a GraphQL API to the [IDS-DRR Frontend](https://github.com/CivicDataLab/IDS-DRR-Frontend).

## Quick start

Using Docker:

```bash
git clone https://github.com/CivicDataLab/IDS-DRR-Data-Management.git
cd IDS-DRR-Data-Management

docker compose up -d --build
docker exec context_layer_Backend python manage.py makemigrations
docker exec context_layer_Backend python manage.py migrate
docker exec context_layer_Backend python manage.py import_geojson
docker exec context_layer_Backend python manage.py import_indicators
docker exec context_layer_Backend python manage.py import_data
```

The API is then available at <http://localhost:8000>.

For full documentation (environment variables, deployment configuration (`config.toml`), and data import schemas), see the [Data Management API docs](https://ids-drr.readthedocs.io/en/latest/platform/data-management.html).

If you want to contribute, please contact us at info@civicdatalab.in.

## License

This project is licensed under the [GNU Affero General Public License v3.0](./LICENSE) (AGPL-3.0) © CivicDataLab.
