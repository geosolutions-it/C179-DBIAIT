from django.conf import settings


class SystemRouter:
    """
    A router to control all database operations on models in the
    system schema
    """
    db_key = 'system'

    def _check_name(self, name):
        if name.startswith("auth_") or name.startswith("django_") or name.startswith("scheduler_"):
            return True
        return False

    def db_for_read(self, model, **hints):
        """
        Attempts to read auth and contenttypes models go to auth_db.
        """
        if self._check_name(model._meta.db_table):
            return self.db_key
        return None

    def db_for_write(self, model, **hints):
        """
        Attempts to write auth and contenttypes models go to auth_db.
        """
        if self._check_name(model._meta.db_table):
            return self.db_key
        return None

    def allow_relation(self, obj1, obj2, **hints):
        """
        Allow relations if a model in the auth or contenttypes apps is
        involved.
        """
        if (
            self._check_name(obj1._meta.db_table) or
            self._check_name(obj2._meta.db_table)
        ):
           return True
        return None

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        """
        Make sure the auth and contenttypes apps only appear in the
        'auth_db' database.
        """
        return True
        #if self._check_name(model_name):
        #    return db == self.db_key
        #return None