

def test_propagation_schema(context, spatial):
    Country, Department, City = spatial

    auth.propagation_schema = {
        'country': 'departments',
        'department': 'cities',
    }