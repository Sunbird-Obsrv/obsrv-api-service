# Suspending a job in kubernetes
# def _suspend_job(self, job_name, namespace, results):
#     result_status = False
#     patch_cmd = """kubectl patch job/{job_name} --type=strategic --patch '{{"spec":{{"suspend":true}}}}' -n {namespace}"""
#     patch_cmd = patch_cmd.format(job_name=job_name, namespace=namespace)
#     patch_cmd_result = subprocess.run(patch_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True,)
#     if patch_cmd_result.returncode == 0:
#         print(f"Job {job_name} suspension succeeded...")
#         result_status = True
#     else:
#         print(f"Error suspending job {job_name}: {patch_cmd_result.stderr.decode()}")
#     results.append(result_status)
#     return results
