import os
import shutil
from mongita import MongitaClientDisk

client = None
db = None
pets = None
owners = None

#Database Connection
def initialize(database_dir="pets_db"):
    global client, db, pets, owners
    client = MongitaClientDisk(host=database_dir)
    db = client["pet_app"]
    pets = db["pets"]
    owners = db["owners"]

def close_connection():
    global client, db, pets, owners
    if client is not None:
        client.close()
    client = None
    db = None
    pets = None
    owners = None

def _normalize_age(value):
    try:
        return int(value)
    except Exception:
        return 0

#Definition
def pet_to_dict(doc):
    return {
        "id": str(doc["_id"]),
        "name": doc["name"],
        "type": doc["type"],
        "age": doc["age"],
        "owner_id":doc["owner_id"]
    }

def owner_to_dict(doc):
    return {
        "id": str(doc["_id"]),
        "name": doc.get("name", ""),
        "email": doc.get("email", "")
    }

#Get All
def get_pets():
    query=list(pets.find({}))
    return [pet_to_dict(pet) for pet in query]

def get_owners():
    query = list(owners.find({}))
    return [owner_to_dict(owner) for owner in query]

#Get single
def get_pet(id):
    pet = pets.find_one({"_id":id})
    if pet is None:
        return None
    return pet_to_dict(pet)

def get_owner(id):
    owner = owners.find_one({"_id": id})
    if owner is None:
        return None
    return owner_to_dict(owner)

#Create
def create_pet(data):
    if "name" not in data:
        raise Exception("Hey! Pet doesn't have name.")
    if data["name"].strip() == "":
        raise Exception("Hey! Pet doesn't have name.")

    owner_id = data.get("owner_id")

    if owner_id != "":
        owner_exists = False
        for owner in owners.find({}):
            if str(owner["_id"]) == str(owner_id):
                owner_exists = True
                break
        if not owner_exists:
            raise Exception("Owner does not exist.")

    pet = pets.insert_one({
        "name": (data.get("name") or "").strip(),
        "type": (data.get("type") or "").strip(),
        "age": _normalize_age(data.get("age")),
        "owner_id": owner_id,
    })
    return pet.inserted_id

def create_owner(data):
    if "name" not in data:
        raise Exception("Hey! Owner doesn't have name.")
    if data["name"].strip() == "":
        raise Exception("Hey! Owner doesn't have name.")

    owner = owners.insert_one({
        "name": (data.get("name") or "").strip(),
        "email": (data.get("email") or "").strip(),
    })
    return owner.inserted_id

#Delete
def delete_pet(id):
    pets.delete_one({"_id":id})

##Need to check pets tables
def delete_owner(id):
    for pet in pets.find({}):
        if str(pet.get("owner_id", "")) == str(id):
            raise Exception("Cannot delete owner: this owner is referenced by a pet.")

    for owner in owners.find({}):
        if str(owner["_id"]) == str(id):
            owners.delete_one({"_id": owner["_id"]})
            return

#Update
def update_pet(id, data):
    if "name" not in data:
        raise Exception("Hey! Pet doesn't have name.")
    if data["name"].strip() == "":
        raise Exception("Hey! Pet doesn't have name.")

    owner_id = data.get("owner_id")

    # check whether owner exists
    if owner_id != "":
        owner_exists = False
        for owner in owners.find({}):
            if str(owner["_id"]) == str(owner_id):
                owner_exists = True
                break

        if not owner_exists:
            raise Exception("Owner does not exist.")

    # We have to find the object
    for pet in pets.find({}):
        if str(pet["_id"]) == str(id):
            pets.update_one(
                {"_id": pet["_id"]},
                {"$set": {
                    "name": (data.get("name") or "").strip(),
                    "type": (data.get("type") or "").strip(),
                    "age": _normalize_age(data.get("age")),
                    "owner_id": owner_id,
                }}
            )
            return

    raise Exception("Pet not found.")

def update_owner(id, data):
    if "name" not in data:
        raise Exception("Hey! Owner doesn't have name.")
    if data["name"].strip() == "":
        raise Exception("Hey! Owner doesn't have name.")

    for owner in owners.find({}):
        if str(owner["_id"]) == str(id):
            owners.update_one(
                {"_id": owner["_id"]},
                {"$set": {
                    "name": (data.get("name") or "").strip(),
                    "email": (data.get("email") or "").strip(),
                }}
            )
            return

    raise Exception("Owner not found.")


def setup_test_database(db_file="test_mongita"):
    close_connection()

    if os.path.exists(db_file):
        shutil.rmtree(db_file)

    initialize(db_file)

    owner_1 = create_owner({"name": "Wayne", "email": "Alice@test.com"})
    owner_2 = create_owner({"name": "Bob", "email": "Bob@test.com"})

    pets_data = [
        {"name": "dorothy", "type": "dog", "age": 9, "owner_id": owner_1},
        {"name": "suzy", "type": "mouse", "age": 9, "owner_id": owner_1},
        {"name": "casey", "type": "dog", "age": 9, "owner_id": owner_2},
        {"name": "heidi", "type": "cat", "age": 15, "owner_id": ""},
    ]
    for pet in pets_data:
        create_pet(pet)

    assert len(get_pets()) == 4
    assert len(get_owners()) == 2

def test_get_pets():
    petAll = get_pets()
    assert type(petAll) is list
    assert len(petAll) >= 1
    assert type(petAll[0]) is dict
    for key in ["id", "name", "type", "age","owner_id"]:
        assert key in petAll[0]
    assert type(petAll[0]["name"]) is str

def test_create_pet_and_get_pet():
    owner_id = create_owner({"name": "Charlie", "email": "Charlie@test.com"})
    new_id = create_pet({"name": "walter", "age": "2", "type": "mouse", "owner_id": owner_id})
    pet = get_pet(new_id)
    assert pet is not None
    assert pet["name"] == "walter"
    assert pet["age"] == 2
    assert pet["type"] == "mouse"
    assert str(pet["owner_id"]) == str(owner_id)

def test_update_pet():
    owner_id = create_owner({"name": "David", "email": "david@test.com"})
    new_id = create_pet({"name": "temp", "age": 1, "type": "cat", "owner_id": ""})
    update_pet(new_id, {"name": "updated", "age": "8", "type": "dog", "owner_id": owner_id})
    pet = get_pet(new_id)
    assert pet is not None
    assert pet["name"] == "updated"
    assert pet["age"] == 8
    assert pet["type"] == "dog"
    assert str(pet["owner_id"]) == str(owner_id)

def test_delete_pet():
    new_id = create_pet({"name": "delete_me", "age": 3, "type": "fish", "owner_id": ""})
    delete_pet(new_id)
    assert get_pet(new_id) is None


if __name__ == "__main__":
    setup_test_database()
    test_get_pets()
    test_create_pet_and_get_pet()
    test_update_pet()
    test_delete_pet()
    close_connection()
    print("done.")
