import numpy as np


def add_ones(data_matrix, n):
    artificial_ones_columns = np.ones((data_matrix.shape[0], n))
    return np.column_stack((artificial_ones_columns, data_matrix))


def nn_model_evaluation(data_matrix, weight_matrix):
    x = []
    z = []
    x_with_ones = []
    n = weight_matrix.shape[0]

    x_with_ones.append(polynomial_basis(data_matrix))

    for i in range(n):
        z.append(x_with_ones[i] @ weight_matrix[i][i:])
        x.append(softmax_function(z[i], axis=1))
        x_with_ones.append(add_ones(x[i], i + 1))

    return x, z


def nn_multinomial_logistic_regression_cost_function(data_matrix, weight_matrix, one_hot_vector_encodings):
    x, z = nn_model_evaluation(data_matrix, weight_matrix)
    term_one = np.sum(np.log(np.sum(np.exp(z[-1]), axis=1)))
    term_two = np.sum(one_hot_vector_encodings * z[-1])
    return term_one - term_two


def nn_multinomial_logistic_regression_gradient(data_matrix, weight_matrix, one_hot_vector_encodings, number_of_layers):
    x, z = nn_model_evaluation(data_matrix, weight_matrix)
    derivative_wrt_x = []
    derivative_wrt_w = []
    derivative_wrt_z = []

    n = weight_matrix.shape[0]
    layers = []
    for i in range(n):
        layers.append(n - i)

    for layer in layers:
        if layer == n:
            derivative_wrt_z.append(x[layer - 1] - one_hot_vector_encodings)
            derivative_wrt_w.append(add_ones(x[layer - 2], layer).T @ derivative_wrt_z[0])
        else:
            derivative_wrt_x.append(derivative_wrt_z[n - layer - 1] @ (weight_matrix[layer][(layer + 1):]).T)
            total_sum = np.sum(x[n - layer - 1] * derivative_wrt_x[n - layer - 1], axis=1)
            derivative_wrt_z.append(x[n - layer - 1] * (derivative_wrt_x[n - layer - 1] - np.vstack([total_sum] * x[n - layer - 1].shape[1]).T))
            derivative_wrt_w.append(add_ones(data_matrix, 1).T @ derivative_wrt_z[layer])

    result = np.empty(number_of_layers, dtype=np.ndarray)
    for i in range(n):
        result[i] = derivative_wrt_w[n - 1 - i]
    return result


def nn_multinomial_prediction_function(data_matrix, weight_matrix):
    x, z = nn_model_evaluation(data_matrix, weight_matrix)
    n_layer = weight_matrix.shape[0]
    probs = x[-1]

    results = np.zeros(data_matrix.shape[0])
    grid = np.arange(weight_matrix[1].shape[1])

    for i in range(probs.shape[0]):
        results[i] = grid[np.argmax(probs[i])]

    return results
