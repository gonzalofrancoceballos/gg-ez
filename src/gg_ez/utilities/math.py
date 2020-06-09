import numpy as np


def solve_linear_equations(a: np.array, b: np.array) -> np.array:
    """
    Solves a system of N linear equations using numpy

    +--------------------------------+
    | a*x = b                        |
    | Where:                         |
    |    a: NxN matrix               |
    |    b: Nx1 vector               |
    |    x: Nx1 vector               |
    +--------------------------------+

    :param a: matrix of constants of shape [N, N]
    :param b: array of constants of shape [N, 1]

    :return: result of equations
    """

    x = np.linalg.inv(a).dot(b)

    return x
