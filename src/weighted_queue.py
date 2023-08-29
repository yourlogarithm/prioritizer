import random


class WeightedQueue:
    def __init__(self):
        self.elements = []  # type: list[tuple[str, float]]
        self.total_weight = 0

    def insert(self, element, weight):
        self.elements.append((element, weight))
        self.total_weight += weight

    def pop(self) -> str | None:
        if not self.elements:
            return None

        r = random.uniform(0, self.total_weight)
        running_weight = 0

        for idx, (element, weight) in enumerate(self.elements):
            running_weight += weight
            if running_weight > r:
                # Remove the selected element from the list and update the total weight
                self.total_weight -= weight
                return self.elements.pop(idx)[0]
        return None
