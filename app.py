from flask import Flask, render_template, request, redirect, url_for
import database

# remember to $ pip install flask
# remember to $ pip install peewee

database.initialize("pets_data")

app = Flask(__name__)


def error_page(message, status=400):
    # Simple text response page, as requested.
    return message, status, {"Content-Type": "text/plain; charset=utf-8"}

#List(pet and owner)
@app.route("/", methods=["GET"])
@app.route("/list", methods=["GET"])
def list():
    pets = database.get_pets()
    owners = database.get_owners()
    return render_template("list.html", pets=pets, owners=owners)

#Create
@app.route("/create", methods=["GET"])
def get_create():
    owners = database.get_owners()
    return render_template("create.html", owners=owners)

@app.route("/owners/create", methods=["GET"])
def pet_owner_create():
    return render_template("create_owner.html")

#Create submit
@app.route("/create", methods=["POST"])
def post_create():
    data = dict(request.form)
    name = (data.get("name") or "").strip()
    age = (data.get("age") or "").strip()
    pet_type = (data.get("type") or "").strip()
    owner_id = (data.get("owner_id") or "").strip()

    if name == "":
        return error_page("Error: name is required.", 400)
    if age == "":
        return error_page("Error: age is required.", 400)
    if pet_type == "":
        return error_page("Error: type is required.", 400)

    database.create_pet(data)
    return redirect(url_for("list"))

@app.route("/owners/create", methods=["POST"])
def post_owner_create():
    data = dict(request.form)
    name = (data.get("name") or "").strip()

    if name == "":
        return error_page("Error: owner name is required.", 400)

    database.create_owner(data)
    return redirect(url_for("list"))

#Delete
@app.route("/delete/<id>", methods=["GET"])
def pet_delete(id):
    database.delete_pet(id)
    return redirect(url_for("list"))

@app.route("/owners/delete/<id>", methods=["GET"])
def pet_owner_delete(id):
    try:
        database.delete_owner(id)
        return redirect(url_for("list"))
    except Exception as e:
        return error_page(str(e), 400)

#Update-select all pet
@app.route("/update/<id>", methods=["GET"])
def pet_update(id):
    pet = database.get_pet(id)
    if pet is None:
        return error_page("Error: pet not found.", 404)

    owners = database.get_owners()
    return render_template("update.html", pet=pet, owners=owners)

#Update-submit
@app.route("/update/<id>", methods=["POST"])
def post_update(id):

    data = dict(request.form)
    name = (data.get("name") or "").strip()
    pet_type = (data.get("type") or "").strip()
    if name == "":
        return error_page("Error: name is required.", 400)
    if pet_type == "":
        return error_page("Error: type is required.", 400)

    try:
        database.update_pet(id, data)
        return redirect(url_for("list"))
    except Exception as e:
        return error_page(str(e), 400)
    
#Update-select all owner
@app.route("/owners/update/<id>", methods=["GET"])
def owner_update(id):
    owner = database.get_owner(id)
    if owner is None:
        return error_page("Error: owner not found.", 404)

    return render_template("update_owner.html", owner=owner)

#Update-submit
@app.route("/owners/update/<id>", methods=["POST"])
def post_owner_update(id):
    data = dict(request.form)
    name = (data.get("name") or "").strip()

    if name == "":
        return error_page("Error: owner name is required.", 400)

    database.update_owner(id, data)
    return redirect(url_for("list"))


@app.route("/health", methods=["GET"])
def health():
    try:
        database.get_pets()
        return error_page("ok", 200)
    except Exception as e:
        return error_page(f"Error checking health: {e}", 500)

if __name__ == "__main__":
    app.run(debug=True)
