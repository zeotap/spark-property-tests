import subprocess

sparkTestingBaseVersions = [
    '2.4.0_0.14.0', '2.4.1_0.14.0', '2.4.2_0.14.0', '2.4.3_0.14.0', '2.4.4_0.14.0', '2.4.5_0.14.0'
]

for version in sparkTestingBaseVersions:
    print(f"Running build for sparkTestBaseVersion: {version}")
    subprocess.call(['sbt' , f'-DsparkTestingBaseVersion={version}', f'-DsparkVersion={version.split("_")[0]}', 'version', 'clean', '+publishLocal'])