from tracardi.service.license import License

if License.has_license():
    import com_tracardi.storage.mysql.interface.destination as destination_dao
    import com_tracardi.storage.mysql.interface.resource as resource_dao
else:
    import tracardi.service.storage.mysql.interface.destination as destination_dao
    import tracardi.service.storage.mysql.interface.resource as resource_dao

__all__ = ['destination_dao', 'resource_dao']
