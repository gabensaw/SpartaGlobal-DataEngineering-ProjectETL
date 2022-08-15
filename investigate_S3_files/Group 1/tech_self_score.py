import csv

test_data = [{"tech_self_score": {"C#": 6, "Java": 5, "R": 2, "JavaScript": 2}},
             {"tech_self_score": {"C": 6, "Java": 5, "Ruby": 2, "JavaScript": 2}}]


def create_tech_self_score_csv(data):
    fields_existing = set({})
    for candidate in test_data:
        for field_existing in candidate["tech_self_score"].keys():
            fields_existing.add(field_existing)

    with open('tech_self_score', 'w') as f:
        writer = csv.writer(f)
        writer.writerow(list(fields_existing))

        for candidate in test_data:
            row = []
            for field_existing in fields_existing:
                if field_existing in candidate["tech_self_score"].keys():
                    row.append(candidate["tech_self_score"][field_existing])
                else:
                    row.append(0)
            writer.writerow(row)


create_tech_self_score_csv(test_data)
