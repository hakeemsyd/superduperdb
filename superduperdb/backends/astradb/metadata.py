import typing as t

import click
import tenacity

from superduperdb import logging
from superduperdb.backends.base.metadata import MetaDataStore
from superduperdb.components.component import Component
from superduperdb.misc.colors import Colors


class AstraMetaDataStore(MetaDataStore):
    """
    Data backend for AstraDB.

    :param conn: AstraDB client connection
    :param name: Name of database to host filesystem
    """

    def __init__(
        self,
        conn: t.Any,
        name: t.Optional[str] = None,
    ) -> None:
        self.name = name
        self.db = conn[name]
        self.meta_collection = self.db['_meta']
        self.cdc_collection = self.db['_cdc_tables']
        self.component_collection = self.db['_objects']
        self.job_collection = self.db['_jobs']
        self.parent_child_mappings = self.db['_parent_child_mappings']
        self.conn = conn

    def url(self):
        """
        Databackend connection url
        """
        return self.db.base_url

    def drop(self, force: bool = False):
        if not force:
            if not click.confirm(
                f'{Colors.RED}[!!!WARNING USE WITH CAUTION AS YOU '
                f'WILL LOSE ALL DATA!!!]{Colors.RESET} '
                'Are you sure you want to drop all meta-data? ',
                default=False,
            ):
                logging.warn('Aborting...')
        self.db.delete_collection(self.meta_collection.name)
        self.db.delete_collection(self.component_collection.name)
        self.db.delete_collection(self.job_collection.name)
        self.db.delete_collection(self.parent_child_mappings.name)

    def create_parent_child(self, parent: str, child: str) -> None:
        self.parent_child_mappings.insert_one(
            document={
                'parent': parent,
                'child': child,
            }
        )

    def create_component(self, info: t.Dict):
        if 'hidden' not in info:
            info['hidden'] = False
        return self.component_collection.insert_one(document=info)

    def create_job(self, info: t.Dict):
        return self.job_collection.insert_one(info)

    # def get_parent_child_relations(self):
    #     c = self.parent_child_mappings.find()
    #     return [(r['parent'], r['child']) for r in c]

    # def get_component_version_children(self, unique_id: str):
    #     return self.parent_child_mappings.distinct('child', {'parent': unique_id})

    def get_job(self, identifier: str):
        return self.job_collection.find_one(filter={'identifier': identifier})

    def create_metadata(self, key: str, value: str):
        return self.meta_collection.insert_one(document={'key': key, 'value': value})

    def get_metadata(self, key: str):
        return self.meta_collection.find_one(filter={'key': key})

    def update_metadata(self, key: str, value: str):
        return self.meta_collection.update_one(filter={'key': key}, update={'$set': {'value': value}})

#need to implemented the distinct feature
    def get_latest_version(
        self, type_id: str, identifier: str, allow_hidden: bool = False
    ) -> int:
        try:
            if allow_hidden:
                    result = self.component_collection.find(filter = {'identifier': identifier, 'type_id': type_id})
                    distinct_values = []
                    for doc in result:
                        if doc['version'] not in distinct_values:
                            distinct_values.append(doc['version'])
                    return sorted(distinct_values)[-1]
            else:
                    result=self.component_collection.find(
                        filter = {
                            '$or': [
                                {
                                    'identifier': identifier,
                                    'type_id': type_id,
                                    'hidden': False,
                                },
                                {
                                    'identifier': identifier,
                                    'type_id': type_id,
                                    'hidden': {'$exists': 0},
                                },
                            ]
                        },
                    )
                    distinct_values = []
                    for doc in result:
                        if doc['version'] not in distinct_values:
                            distinct_values.append(doc['version'])
                    return sorted(distinct_values)[-1]
        except IndexError:
            raise FileNotFoundError(f'Can\'t find {type_id}: {identifier} in metadata')

    def update_job(self, identifier: str, key: str, value: t.Any):
        return self.job_collection.update_one(
            filter={'identifier': identifier},update={'$set': {key: value}}
        )

    #simplemented the distinct feature
    def show_components(self, type_id: str, **kwargs) -> t.List[t.Union[t.Any, str]]:
        # TODO: Should this be sorted?
        result = self.component_collection.find(
            filter = {'type_id': type_id, **kwargs}
        )
        distinct_values = []
        for doc in result:
            if doc['identifier'] not in distinct_values:
                distinct_values.append(doc['identifier'])
        return distinct_values



    #implemented the distinct feature
    # TODO: Why is this is needed to prevent failures in CI?
    @tenacity.retry(stop=tenacity.stop_after_attempt(10))
    def show_component_versions(
        self, type_id: str, identifier: str
    ) -> t.List[t.Union[t.Any, int]]:
        result = self.component_collection.distinct(
            filter = {'type_id': type_id, 'identifier': identifier}
        )
        distinct_values = []
        for doc in result:
            if doc['version'] not in distinct_values:
                distinct_values.append(doc['version'])
        return distinct_values


    # def list_components_in_scope(self, scope: str):
    #     out = []
    #     for r in self.component_collection.find(filter={'parent': scope}):
    #         out.append((r['type_id'], r['identifier']))
    #     return out

    def show_job(self, job_id: str):
        return self.job_collection.find_one(filter={'identifier': job_id})

    def show_jobs(self, status=None):
        status = {} if status is None else {'status': status}
        return list(
            self.job_collection.find(
                filter = {'status': status ,'identifier': 1, '_id': 0, 'method': 1, 'status': 1, 'time': 1}
            )
        )

    def _component_used(
        self, type_id: str, identifier: str, version: t.Optional[int] = None
    ) -> bool:
        if version is None:
            members: t.Union[t.Dict, str] = {'$regex': f'^{identifier}/{type_id}'}
        else:
            members = Component.make_unique_id(type_id, identifier, version)

        return bool(self.component_collection.count_documents(filter={'members': members}))

    # def component_has_parents(self, type_id: str, identifier: str) -> int:
    #     doc = {'child': {'$regex': f'^{type_id}/{identifier}/'}}
    #     return self.parent_child_mappings.count_documents(doc)

    def component_version_has_parents(
        self, type_id: str, identifier: str, version: int
    ) -> int:
        doc = {'child': Component.make_unique_id(type_id, identifier, version)}
        return self.parent_child_mappings.count_documents(filter=doc)

    def delete_component_version(
        self, type_id: str, identifier: str, version: int
    ):
        if self._component_used(type_id, identifier, version=version):
            raise Exception('Component version already in use in other components!')

        self.parent_child_mappings.delete_many(
            {'parent': Component.make_unique_id(type_id, identifier, version)}
        )

        return self.component_collection.delete_many(
            filter={
                'identifier': identifier,
                'type_id': type_id,
                'version': version,
            }
        )

    def _get_component(
        self,
        type_id: str,
        identifier: str,
        version: int,
        allow_hidden: bool = False,
    ) -> t.Dict[str, t.Any]:
        if not allow_hidden:
            r =  self.component_collection.find_one(
                filter={
                    '$or': [
                        {
                            'identifier': identifier,
                            'type_id': type_id,
                            'version': version,
                            'hidden': False,
                        },
                        {
                            'identifier': identifier,
                            'type_id': type_id,
                            'version': version,
                            'hidden': {'$exists': 0},
                        },
                    ]
                }
            )
        else:
            r = self.component_collection.find_one(
                filter={
                    'identifier': identifier,
                    'type_id': type_id,
                    'version': version,
                },
            )
        return r

    def get_component_version_parents(self, unique_id: str) -> t.List[str]:
        return [
            r['parent'] for r in self.parent_child_mappings.find(filter={'child': unique_id})
        ]

    def _replace_object(
        self,
        info: t.Dict[str, t.Any],
        identifier: str,
        type_id: str,
        version: int,
    ) -> None:
        self.component_collection.find_one_and_replace(
            filter={'identifier': identifier, 'type_id': type_id, 'version': version},
            replacement=info,
        )

    def _update_object(
        self,
        identifier: str,
        type_id: str,
        key: str,
        value: t.Any,
        version: int,
    ):
        return self.component_collection.update_one(
            filter={'identifier': identifier, 'type_id': type_id, 'version': version},
            update={'$set': {key: value}},
        )

    def write_output_to_job(self, identifier, msg, stream):
        if stream not in ('stdout', 'stderr'):
            raise ValueError(f'stream is "{stream}", should be stdout or stderr')
        self.job_collection.update_one(
            filter={'identifier': identifier},update= {'$push': {stream: msg}}
        )

    def hide_component_version(
        self, type_id: str, identifier: str, version: int
    ) -> None:
        self.component_collection.update_one(
            filter={'type_id': type_id, 'identifier': identifier, 'version': version},
            update={'$set': {'hidden': True}},
        )

    def disconnect(self):
        """
        Disconnect the client
        """

        # TODO: implement me
