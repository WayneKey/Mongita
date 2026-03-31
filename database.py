import os
import shutil
from mongita import MongitaClientDisk

client = None
db = None
pets = None

def initialize(database_dir="pets_db"):
    global client, db, pets
    client = MongitaClientDisk(host=database_dir)
    db = client["pet_app"]
    pets = db["pets"]

def close_connection():
    global client, db, pets
    if client is not None:
        client.close()
    client = None
    db = None
    pets = None

def _normalize_age(value):
    try:
        return int(value)
    except Exception:
        return 0

def pet_to_dict(doc):
    return {
        "id": doc["_id"],
        "name": doc["name"],
        "type": doc["type"],
        "age": doc["age"],
    }

def get_pets():
    query=list(pets.find({}))
    return [pet_to_dict(pet) for pet in query]

def get_pet(id):
    pet = pets.find_one({"_id":id})
    if pet is None:
        return None
    return pet_to_dict(pet)

def create_pet(data):
    if "name" not in data:
        raise Exception("Hey! Pet doesn't have name.")
    if data["name"].strip()=="":
        raise Exception("Hey! Pet doesn't have name.")
    
    pet = pets.insert_one({
        "name":(data.get("name") or "").strip(),
        "type":(data.get("type") or "").strip(),
        "age":_normalize_age(data.get("age")),
    })
    return pet.id

def delete_pet(id):
    pets.delete_one({"_id":id})

def update_pet(id, data):
    if "name" not in data:
        raise Exception("Hey! Pet doesn't have name.")
    if data["name"].strip()=="":
        raise Exception("Hey! Pet doesn't have name.")

    pets.update_one({"_id":id},{
        "name":(data.get("name") or "").strip(),
        "type":(data.get("type") or "").strip(),
        "age":_normalize_age(data.get("age")),
    })

def setup_test_database(db_file="test_mongita"):
    close_connection()

    if os.path.exists(db_file):
        shutil.rmtree(db_file)

    initialize(db_file)

    pets = [
        {"name": "dorothy", "type": "dog", "age": 9},
        {"name": "suzy", "type": "mouse", "age": 9},
        {"name": "casey", "type": "dog", "age": 9},
        {"name": "heidi", "type": "cat", "age": 15},
    ]
    for pet in pets:
        create_pet(pet)

    assert len(get_pets()) == 4

def test_get_pets():
    petAll = get_pets()
    assert type(petAll) is list
    assert len(petAll) >= 1
    assert type(petAll[0]) is dict
    for key in ["id", "name", "type", "age"]:
        assert key in pets[0]
    assert type(pets[0]["name"]) is str

def test_create_pet_and_get_pet():
    new_id = create_pet({"name": "walter", "age": "2", "type": "mouse"})
    pet = get_pet(new_id)
    assert pet is not None
    assert pet["name"] == "walter"
    assert pet["age"] == 2
    assert pet["type"] == "mouse"

def test_update_pet():
    new_id = create_pet({"name": "temp", "age": 1, "type": "cat"})
    update_pet(new_id, {"name": "updated", "age": "8", "type": "dog"})
    pet = get_pet(new_id)
    assert pet is not None
    assert pet["name"] == "updated"
    assert pet["age"] == 8
    assert pet["type"] == "dog"

def test_delete_pet():
    new_id = create_pet({"name": "delete_me", "age": 3, "type": "fish"})
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
