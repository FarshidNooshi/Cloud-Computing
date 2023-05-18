import subprocess

# Number of times to run the cURL command
num_runs = 1000

# Dictionary to track the usage count for each pod
pod_usage_count = {}

for _ in range(num_runs):
    # Execute the cURL command to shorten the URL
    result = subprocess.run(['curl', '-X', 'POST', 'http://localhost/shorten_url',
                             '-H', 'Content-Type: application/json',
                             '-d', '{"longURL": "https://FarshidNooshi.GitHub.io"}'],
                            capture_output=True, text=True)

    # replace the true with True in result.stdout
    result.stdout = result.stdout.replace('true', 'True')
    result.stdout = result.stdout.replace('false', 'False')

    # the stdout type is json, convert it to dict
    dict_result = eval(result.stdout)

    # Extract the pod hostname from the cURL response
    if 'hostname' in result.stdout:
        hostname = dict_result['hostname']
        pod_usage_count[hostname] = pod_usage_count.get(hostname, 0) + 1

# Print the pod usage count
for pod, count in pod_usage_count.items():
    print(f"Pod: {pod}, Usage Count: {count}")
