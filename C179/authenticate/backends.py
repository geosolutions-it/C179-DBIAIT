from django_auth_ldap.backend import LDAPBackend


class MyLDAPBackend(LDAPBackend):
    """ A custom LDAP authentication backend """

    def authenticate(self, username, password):
        """ Overrides LDAPBackend.authenticate to add custom logic """

        user = LDAPBackend().authenticate(self, username, password)

        """ Add custom logic here """
        return user