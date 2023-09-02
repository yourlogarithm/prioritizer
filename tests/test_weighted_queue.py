import unittest
from scipy.stats import chisquare
import numpy as np
from src.weighted_queue import WeightedQueue


class TestWeightedQueue(unittest.TestCase):
    def test_initialization(self):
        queue = WeightedQueue()
        self.assertEqual(queue.elements, [])
        self.assertEqual(queue.total_weight, 0)

    def test_insert(self):
        queue = WeightedQueue()
        queue.insert("element1", 1)
        self.assertEqual(queue.elements, [("element1", 1)])
        self.assertEqual(queue.total_weight, 1)

    def test_pop(self):
        queue = WeightedQueue()
        queue.insert("element1", 1)
        popped_element = queue.pop()
        self.assertEqual(popped_element, "element1")
        self.assertEqual(queue.total_weight, 0)

    def test_pop_empty_queue(self):
        queue = WeightedQueue()
        popped_element = queue.pop()
        self.assertIsNone(popped_element)
        self.assertEqual(queue.total_weight, 0)

    def test_randomness(self):
        queue = WeightedQueue()
        elements = ["element1", "element2", "element3"]
        weights = [1, 2, 3]

        for element, weight in zip(elements, weights):
            queue.insert(element, weight)

        n_trials = 10000
        observed = {element: 0 for element in elements}

        for _ in range(n_trials):
            popped_element = queue.pop()
            if popped_element is not None:
                observed[popped_element] += 1
                # Re-insert the popped element to maintain the queue size
                index = elements.index(popped_element)
                queue.insert(popped_element, weights[index])

        # Use the chi-squared test for goodness-of-fit
        observed_values = np.array(list(observed.values()))
        expected_values = np.array(weights) * (n_trials / sum(weights))

        chi2, p_value = chisquare(observed_values, expected_values)

        self.assertGreater(p_value, 0.05)
