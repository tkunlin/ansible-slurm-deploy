# ansible-slurm-deploy

## Prerequisite
* Deploy ssh-eky  to each node
* Each node need to sync time 


## Deployment
```
deploy_slurm.sh
```

## Verify 

* smoke test
```
./slurm_smoke_and_stress.sh smoke
```

* stress test 60 seconds
```
./slurm_smoke_and_stress.sh stress -t 60
```


* smoke test
```
./slurm_smoke_and_stress.sh smoke
```

* Real test
```
./slurm_value_tests.sh pi -n 3
./slurm_value_tests.sh prime -n 3 --ntasks-per-node 2
./slurm_value_tests.sh stress-pi -t 60 -n 3 -c 2
```

* Stage 2 Test
```
./slurm_stage2_validation.sh mem -n 3 --mem-mb 1024 -t 300
./slurm_stage2_validation.sh io -n 3 --io-mb 1024
./slurm_stage2_validation.sh burnin -n 3 -c 2 -t 1800 --mem-mb 1024 --io-mb 128
```
