package pdc;

import java.util.Random;
import java.util.Arrays;

/**
 * MatrixGenerator: utility class for generating and printing matrices.
 * Combines original functionality with improved modern methods.
 */
public class MatrixGenerator {

    private static final Random RANDOM = new Random();

    /** Generates a random matrix (original style). */
    public static int[][] generateRandomMatrix(int rows, int cols, int maxValue) {
        int[][] matrix = new int[rows][cols];
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                matrix[i][j] = RANDOM.nextInt(maxValue);
            }
        }
        return matrix;
    }

    /** Generates a random matrix using modern stream approach. */
    public static int[][] randomMatrix(int rows, int cols, int maxValue) {
        int[][] matrix = new int[rows][cols];
        for (int i = 0; i < rows; i++) {
            matrix[i] = RANDOM.ints(cols, 0, maxValue).toArray();
        }
        return matrix;
    }

    /** Generates an identity matrix. */
    public static int[][] generateIdentityMatrix(int size) {
        int[][] matrix = new int[size][size];
        for (int i = 0; i < size; i++) {
            matrix[i][i] = 1;
        }
        return matrix;
    }

    /** Generates a matrix filled with a specific value (original). */
    public static int[][] generateFilledMatrix(int rows, int cols, int value) {
        int[][] matrix = new int[rows][cols];
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                matrix[i][j] = value;
            }
        }
        return matrix;
    }

    /** Generates a matrix filled with a specific value using Arrays.fill(). */
    public static int[][] filledMatrix(int rows, int cols, int value) {
        int[][] matrix = new int[rows][cols];
        for (int i = 0; i < rows; i++) {
            Arrays.fill(matrix[i], value);
        }
        return matrix;
    }

    /** Prints a matrix with an optional label. */
    public static void printMatrix(int[][] matrix, String label) {
        if (label != null && !label.isEmpty()) {
            System.out.println(label);
        }
        for (int[] row : matrix) {
            for (int val : row) {
                System.out.printf("%6d", val);
            }
            System.out.println();
        }
    }

    /** Prints a matrix without a label. */
    public static void printMatrix(int[][] matrix) {
        printMatrix(matrix, "");
    }


}