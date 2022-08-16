
def create_tech_self_score_list(cand_id, scores):
    # List of lists to store tech self scores
    rows = []

    for skill in scores[0]:
        # One row for each skill
        row = [cand_id, skill, scores[0][skill]]
        rows.append(row)
    return rows

