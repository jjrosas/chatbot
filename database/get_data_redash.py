import requests
import json
import time


def poll_job(s, redash_url, job):
    # TODO: add timeout
    while job["status"] not in (3, 4):
        response = s.get("{}/api/jobs/{}".format(redash_url, job["id"]))
        job = response.json()["job"]
        time.sleep(1)

    if job["status"] == 3:
        return job["query_result_id"]

    return None


def get_query_result(
    redash_url, query_id, api_key, params=None, cached=True, max_age=None
):  # , params):
    """
    gets query results for a specified query id. can be cached results or new ones
    :param: redash_url, Redash base URL to append endpoint to
    :param: query_id, ID of the query to get results from
    :param: cached, True if acceptable to bring last cached results (default)
    :param: max_age, max age (in seconds) for which is acceptable to return a cached result. max_age=0 forces refresh.
    """
    s = requests.Session()
    s.headers.update({"Authorization": "Key {}".format(api_key)})

    if cached:
        response = s.get("{}/api/queries/{}/results".format(redash_url, query_id))
        return response.json()
    else:
        if max_age is not None:
            if params is not None:
                payload = dict(max_age=max_age, parameters=params)
            else:
                payload = dict(max_age=max_age)

            response = s.post(
                "{}/api/queries/{}/results".format(redash_url, query_id),
                data=json.dumps(payload),
            )

            if response.status_code != 200:
                raise Exception("Refresh failed.")

            result_id = poll_job(s, redash_url, response.json()["job"])

            if result_id:
                response = s.get(
                    "{}/api/queries/{}/results/{}.json".format(
                        redash_url, query_id, result_id
                    )
                )
                if response.status_code != 200:
                    raise Exception("Failed getting results.")
            else:
                raise Exception("Query execution failed.")

            return response.json()
        else:
            raise Exception("if cached=False then max_age needs to be specified")
