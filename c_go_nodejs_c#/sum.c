#include <stdio.h>
#include <time.h>

int main() {
    long long sum = 0;
    clock_t start = clock();

    for (int i = 1; i <= 100000000; i++) {
        sum += i;
    }

    clock_t end = clock();
    double time_spent = (double)(end - start) / CLOCKS_PER_SEC;

    printf("Sum: %lld\n", sum);
    printf("Time: %.3f seconds\n", time_spent);

    return 0;
}
