import csv

def compute_average_invalid_lines_ratio(csv_file):
    total_invalid_lines = 0
    total_lines = 0

    with open(csv_file, 'r') as file:
        reader = csv.reader(file)
        next(reader)  # Skip header

        for row in reader:
            invalid_lines = int(row[1])
            total_invalid_lines += invalid_lines
            total_lines += int(row[2])

    average_ratio = (total_invalid_lines / total_lines) * 100
    return average_ratio

csv_file = 'beara_cnt_lines.csv'
average_ratio = compute_average_invalid_lines_ratio(csv_file)
print(f"Average Invalid Lines Ratio: {average_ratio:.2f}%")
