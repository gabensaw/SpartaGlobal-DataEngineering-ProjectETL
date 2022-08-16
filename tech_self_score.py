test_data = {"tech_self_score": {"C#": 6, "Java": 5, "R": 2, "JavaScript": 2}}

list1 = test_data["tech_self_score"]

def create_tech_self_score_list(cand_id, scores):
    # List of lists to store tech self scores
    rows = []

    for skill in scores:
        print(skill)

        # One row for each skill
        row = [cand_id, skill, scores[skill]]
        rows.append(row)
    return rows


print(create_tech_self_score_list(1, list1))
