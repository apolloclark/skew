# Copyright (c) 2014 Scopely, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.
import jmespath
import logging
from botocore.exceptions import ClientError
from skew.resources.aws import AWSResource

LOG = logging.getLogger(__name__)


class Bucket(AWSResource):

    _location_cache = {}

    @classmethod
    def enumerate(cls, arn, region, account, resource_id=None, **kwargs):
        resources = super(Bucket, cls).enumerate(arn, region, account,
                                                 resource_id,
                                                 **kwargs)
        region_resources = []
        if region is None:
            region = 'us-east-1'
        for r in resources:
            location = cls._location_cache.get(r.id)
            if location is None:
                LOG.debug('finding location for %s', r.id)
                kwargs = {'Bucket': r.id}
                response = r._client.call('get_bucket_location', **kwargs)
                location = response.get('LocationConstraint', 'us-east-1')
                if location is None:
                    location = 'us-east-1'
                if location is 'EU':
                    location = 'eu-west-1'
                cls._location_cache[r.id] = location
            if location == region:
                region_resources.append(r)
        return region_resources

    class Meta(object):
        service = 's3'
        type = 'bucket'
        enum_spec = ('list_buckets', 'Buckets[]', None)
        detail_spec = ('list_objects', 'Bucket', 'Contents[]')
        attr_spec = [
            ('get_bucket_accelerate_configuration', 'Bucket',
                None, 'AccelerateConfiguration'),
            ('get_bucket_acl', 'Bucket',
                None, 'BucketAcl'),
            ('get_bucket_cors', 'Bucket', None, 'Cors'),
            ('get_bucket_encryption', 'Bucket', None, 'Encryption'),
            ('get_bucket_lifecycle_configuration', 'Bucket',
                None, 'LifecycleConfiguration'),
            ('get_bucket_location', 'Bucket',
                None, 'Location'),
            ('get_bucket_logging', 'Bucket',
                None, 'LoggingEnabled'),
            ('get_bucket_notification_configuration', 'Bucket',
                None, 'TopicConfigurations'),
            ('get_bucket_policy', 'Bucket',
                None, 'Policy'),
            ('get_bucket_policy_status', 'Bucket',
                None, 'PolicyStatus'),
            ('get_bucket_replication', 'Bucket',
                None, 'ReplicationConfiguration'),
            ('get_bucket_request_payment', 'Bucket',
                None, 'Payer'),
            ('get_bucket_versioning', 'Bucket',
                None, 'Versioning'),
            ('get_bucket_website', 'Bucket',
                None, 'Website'),
        ]
        id = 'Name'
        filter_name = None
        name = 'BucketName'
        date = 'CreationDate'
        dimension = None
        tags_spec = ('get_bucket_tagging', 'TagSet[]',
                     'Bucket', 'id')

    def __init__(self, client, data, query=None):
        super(Bucket, self).__init__(client, data, query)
        self._data = data
        self._keys = []
        self._id = data['Name']

        # add addition attribute data
        for attr in self.Meta.attr_spec:
            detail_op, param_name, detail_path, detail_key = attr
            params = {param_name: self._id}
            data = self._client.call(detail_op, **params)
            if not (detail_path is None):
                data = jmespath.search(detail_path, data)
            if 'ResponseMetadata' in data:
                del data['ResponseMetadata']
            self.data[detail_key] = data

    def __iter__(self):
        detail_op, param_name, detail_path = self.Meta.detail_spec
        params = {param_name: self.id}
        if not self._keys:
            data = self._client.call(detail_op, **params)
            self._keys = jmespath.search(detail_path, data)
        for key in self._keys:
            yield key
