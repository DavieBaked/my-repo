import pytest
import os
import re

def pytest_sessionstart(session):
    session.file_group_scores = {}  # Tracks scores for each file
    session.question_file_groups = {}  # Tracks pass/fail status for question groups within files
    session.failed_tests_by_file = {}  # Dictionary to track failed tests organized by file name


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item):
    outcome = yield
    rep = outcome.get_result()

    # Extract the file name and question group from the test item
    file_name = os.path.basename(item.location[0])
    match = re.search(r'question_(\d+)', item.name)
    question_group = match.group(0) if match else None

    # Combine file name and question group for unique identification
    group_identifier = f"{file_name}_{question_group}" if question_group else file_name

    if rep.when == 'call' or rep.when == 'setup':
        if group_identifier not in item.session.question_file_groups:
            item.session.question_file_groups[group_identifier] = True

        # Track failed tests and errors
        if rep.failed:
            item.session.question_file_groups[group_identifier] = False
            test_identifier = f"{file_name}::{item.name}"
            # Append the test identifier to failed_tests_by_file
            item.session.failed_tests_by_file.setdefault(file_name, []).append(test_identifier)



def pytest_sessionfinish(session, exitstatus):
    for group in session.question_file_groups:
        file_name = group.split('_')[0]
        session.file_group_scores.setdefault(file_name, []).append(session.question_file_groups[group])

    print("\n\nScores per file:")
    grand_total = 0
    for file, scores in session.file_group_scores.items():
        file_score = sum(1 for passed in scores if passed)
        grand_total += file_score
        print(f"{file}: {file_score}")

    print(f'\nTotal score: {grand_total}/12')

    # Print the failed test identifiers by file
    if session.failed_tests_by_file:
        print("\nFailed tests by file:")
        for file, test_identifiers in session.failed_tests_by_file.items():
            print(f"\n{file}:")
            for test_identifier in test_identifiers:
                print(f"  - {test_identifier}")





