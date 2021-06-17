import pickle


def get_access_token():
    with open("core/access_token", "rb") as f:
        data = pickle.load(f)
        return data["access_token"]


def get_instrument_data():
    with open("core/instrument_data", "rb") as f:
        instrument_data = pickle.load(f)
    return instrument_data