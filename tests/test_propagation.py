from jsalchemy_auth import Auth


def test_propagation_schema(context, spatial, auth: Auth):
    Country, Department, City = spatial

    auth.propagation_schema = {
        'country': 'departments',
        'department': 'cities',
    }
