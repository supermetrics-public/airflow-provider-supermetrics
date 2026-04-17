"""Supermetrics login operators for Apache Airflow."""

from __future__ import annotations

from typing import Any, Sequence

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator

from airflow.providers.supermetrics.hooks.supermetrics import SupermetricsHook
from supermetrics import AuthenticationError, NetworkError, ValidationError


class SupermetricsCreateLoginLinkOperator(BaseOperator):
    """Create a Supermetrics login link for a data source.

    Generates a login link that can be used to authorize access to
    a data source. Returns a dict with ``login_id`` and ``link_url``,
    and also pushes them as separate XCom values.
    """

    template_fields: Sequence[str] = ("ds_id", "description")
    ui_color = "#00b4d8"

    def __init__(
        self,
        *,
        ds_id: str,
        description: str | None = None,
        supermetrics_conn_id: str = SupermetricsHook.default_conn_name,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.ds_id = ds_id
        self.description = description
        self.supermetrics_conn_id = supermetrics_conn_id

    def execute(self, context: Any) -> dict[str, str]:
        hook = SupermetricsHook(supermetrics_conn_id=self.supermetrics_conn_id)
        client = hook.get_conn()

        try:
            result = client.login_links.create(
                ds_id=self.ds_id,
                description=self.description,
            )
        except AuthenticationError as e:
            raise AirflowException(f"Supermetrics authentication failed: {e}") from e
        except ValidationError as e:
            raise AirflowException(f"Supermetrics validation error: {e}") from e
        except NetworkError as e:
            raise AirflowException(f"Supermetrics network error: {e}") from e

        login_id: str = result.login_id
        link_url: str = result.login_url

        ti = context["ti"]
        ti.xcom_push(key="login_id", value=login_id)
        ti.xcom_push(key="link_url", value=link_url)

        self.log.info("Created login link %s for data source %s", login_id, self.ds_id)
        return {"login_id": login_id, "link_url": link_url}


class SupermetricsListLoginsOperator(BaseOperator):
    """List all Supermetrics data source logins.

    Returns a list of dicts, each representing a data source login.
    """

    template_fields: Sequence[str] = ()
    ui_color = "#00b4d8"

    def __init__(
        self,
        *,
        supermetrics_conn_id: str = SupermetricsHook.default_conn_name,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.supermetrics_conn_id = supermetrics_conn_id

    def execute(self, context: Any) -> list[dict[str, Any]]:
        hook = SupermetricsHook(supermetrics_conn_id=self.supermetrics_conn_id)
        client = hook.get_conn()

        try:
            result = client.logins.list()
        except AuthenticationError as e:
            raise AirflowException(f"Supermetrics authentication failed: {e}") from e
        except ValidationError as e:
            raise AirflowException(f"Supermetrics validation error: {e}") from e
        except NetworkError as e:
            raise AirflowException(f"Supermetrics network error: {e}") from e

        logins: list[dict[str, Any]] = [login.to_dict() for login in result]
        self.log.info("Found %d logins", len(logins))
        return logins


class SupermetricsGetLoginOperator(BaseOperator):
    """Get a Supermetrics data source login by ID.

    Returns a dict representing the data source login.
    """

    template_fields: Sequence[str] = ("login_id",)
    ui_color = "#00b4d8"

    def __init__(
        self,
        *,
        login_id: str,
        supermetrics_conn_id: str = SupermetricsHook.default_conn_name,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.login_id = login_id
        self.supermetrics_conn_id = supermetrics_conn_id

    def execute(self, context: Any) -> dict[str, Any]:
        hook = SupermetricsHook(supermetrics_conn_id=self.supermetrics_conn_id)
        client = hook.get_conn()

        try:
            result = client.logins.get(login_id=self.login_id)
        except AuthenticationError as e:
            raise AirflowException(f"Supermetrics authentication failed: {e}") from e
        except ValidationError as e:
            raise AirflowException(f"Supermetrics validation error: {e}") from e
        except NetworkError as e:
            raise AirflowException(f"Supermetrics network error: {e}") from e

        login_data: dict[str, Any] = result.to_dict()
        self.log.info("Retrieved login %s", self.login_id)
        return login_data
