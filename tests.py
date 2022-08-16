import unittest
import tech_self_score


class MyTestCase(unittest.TestCase):
    def test_create_self_score_lst(self):

        test_data = {"tech_self_score": {"C#": 6, "Java": 5, "R": 2, "JavaScript": 2}}
        test_id = 1
        test_list = test_data["tech_self_score"]
        
        output = tech_self_score.create_tech_self_score_list(test_id, test_list)
        expected = [[1, 'C#', 6], [1, 'Java', 5], [1, 'R', 2], [1, 'JavaScript', 2]]

        self.assertEqual(output, expected)

if __name__ == '__main__':
    unittest.main()
