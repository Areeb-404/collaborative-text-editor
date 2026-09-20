def create_initial_document() :
    return [
        {"key":(1,"client_0"), "char" : "H", "deleted" : False},
        {"key":(2,"client_0"), "char" : "e", "deleted" : False},
        {"key":(3,"client_0"), "char" : "l", "deleted" : False},
        {"key":(4,"client_0"), "char" : "o", "deleted" : False},
        {"key":(5,"client_0"), "char" : "o", "deleted" : False},
    ]

def apply_insert(document,key,char):
    new_character_node = {
        "key" : key,
        "char" : char,
        "deleted" : False
    }
    document.append(new_character_node)

def apply_delete(document,target_key):
    for character_node in document:
        if character_node["key"] == target_key:
            character_node["deleted"] = True
            break

def apply_operation(document,operation):
    operation_type = operation["type"]
    if operation_type=="insert":
        apply_insert(document,operation["key"],operation["char"])
    elif operation_type=="delete":
        apply_delete(document,operation["target_key"])


def render_document(document):
    active_character_nodes = []
    for character_node in document:
        if not character_node["deleted"]:
            active_character_nodes.append(character_node)

    sorted_character_nodes = sorted(
        active_character_nodes,
        key= lambda item: item["key"]
    )
    # the lambda function simply tells sorted what to base the sorting on
    # the sorted function ensures that characters are always reproduced in the same order

    rendered_characters = []
    for character_node in sorted_character_nodes:
        rendered_characters.append(character_node["char"])

    return "".join(rendered_characters)


def run_crdt_simulation():

    # initialising both the users with their base document
    replica_a_document = create_initial_document()
    replica_b_document = create_initial_document()

    # the types of operations to be performed on both documents
    operation_1 = {"type" : "insert", "key" : (1.5,"client_A"), "char" : "a"}
    operation_2 = {"type" : "insert", "key" : (2.5,"client_A"), "char" : "p"}
    operation_3 = {"type" : "insert", "key" : (2.5,"client_B"), "char" : "!"}
    operation_4 = {"type" : "delete", "target_key" : (3.0,"client_0")}

    replica_a_operations = [operation_1,operation_2,operation_3,operation_4]
    replica_b_operations = [operation_4,operation_2,operation_1,operation_3]
    # both the users recieved a different order of operation for the same task


    for operation in replica_a_operations:
        apply_operation(replica_a_document,operation)


    for operation in replica_b_operations:
        apply_operation(replica_b_document,operation)


    rendered_text_a = render_document(replica_a_document)
    rendered_text_b = render_document(replica_b_document)

    assert rendered_text_a == rendered_text_b
    # assert checks if the given condition is true then it does nothing, if condition is false it gives an assertion error, its just a way to rigorously check an important condition

    print("Replica A final text: ",rendered_text_a)
    print("Replica b final text: ",rendered_text_b)
    print("Assertion Passed: Both replicas produced identical string output.")


if __name__ == "__main__":
    run_crdt_simulation()
