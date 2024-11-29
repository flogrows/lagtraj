import cdsapi
from cdsapi.api import Result
from cads_api_client import legacy_api_client

# Although, cads_api_client now deprecated to datapi so maybe should use
# from datapi import legacy_api_client 


class RequestFetchCDSClient(legacy_api_client.LegacyApiClient): 
    class RequestNotFoundException(Exception):
        pass

    """
    Wraps CDS api so that we can submit a request, get the request id and then
    later query the status or download data based on a request ID.
    """
    def __new__(cls, *args, **kwargs): # Need to use __new__ here or just get LegacyApiClient object
        return object.__new__(cls)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def queue_data_request(self, repository_name, query_kwargs):
        response = self.retrieve(repository_name, query_kwargs)

        if response.response.status_code not in [200, 202]: # response is a requests.Results object
            raise Exception(
                "Something went wrong requesting the data: {}"
                "".format(response.json())
            )
        else:
            # This doesn't work at all, new Results object is totally different, I can't tell if it actually contains the request id
            # See https://github.com/ecmwf-projects/cads-api-client/blob/cb96ceb38f599f42fff65392035031e2e697e119/cads_api_client/processing.py#L624
            
            reply = response.json()
            return reply["request_id"]

    def download_data_by_request(self, request_id, target):
        reply = self._get_request_status(request_id=request_id)

        result = Result(client=self, reply=reply)
        result.download(target=target)

    def _get_request_status(self, request_id):
        task_url = "{}/tasks/{}".format(self.url, request_id)
        session = self.session
        result = self.robust(session.get)(
            task_url, verify=self.verify, timeout=self.timeout
        )
        return result.json()

    def get_request_status(self, request_id):
        reply = self._get_request_status(request_id=request_id)
        if "state" not in reply:
            if reply["message"] == "Not found":
                raise self.RequestNotFoundException
            else:
                raise NotImplementedError(reply)
        else:
            return reply["state"]
