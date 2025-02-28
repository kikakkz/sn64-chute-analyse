# sn64-chute-analyse

## Run chute audit

- https://github.com/rayonlabs/chutes-audit

## Setting up password-free login

- set up password-free login to primary host
- set up password-free login to chutes audit host

## Edit your config

- refer to config.template file

## Run main.py

- python3 main.py -c config.template

Local sqlite is used to store running history of instances, so if you need to
watch a full history you should run above command with a `while` loop with proper interval
you want.

## Result

|    Host IP    | Active | GPU Type | Compute Units 1 hour | Compute Units 1 day | Compute Units 7 days |
|----|----|----|----|----|----|
|  x.x.x.x |  True  | h100_sxm |  174.71656393150687  |  5675.005345068492  |   10317.9046619589   |

|    Host IP    | GPU Type | GPU Count |              Instace ID              |               Chute ID               |            Deployment ID             |       Running Time      | End Time | Compute Units 1 hour | Compute Units 1 day | Compute Units 7 days |
|----|----|----|----|----|----|----|----|----|----|----|
|  x.x.x.x | h100_sxm |     1     | 0404cea2-a2ef-4f67-9449-339c6cc5f8ca | 2b3d74b5-fa25-551c-9ea0-240949530eb7 | c9a1b5ed-c759-4e95-aaf9-1efbe5453303 |      3:16:13.791404     |    0     |  13.766444876712331  |  49.61518964383562  |  49.61518964383562   |
|  x.x.x.x | h100_sxm |     1     | 3fe179d4-e695-4b9e-a46d-ccd5d733e806 | 2b3d74b5-fa25-551c-9ea0-240949530eb7 | d784d0ec-f045-4bc1-be85-93e9db571d69 |  2 days, 8:33:30.792205 |    0     |  16.44124795890411   |  321.64770156164406 |   552.535295178082   |
|  x.x.x.x | h100_sxm |     1     | 552a4a62-4d1a-49c9-99de-e58c25a793ba | 10f50209-e346-557c-87d1-ab32f7b21f0a | 048935a6-23cf-4162-97d9-927e5a068e30 |  2 days, 8:34:33.788412 |    0     |  31.834669931506852  |  1686.1573935616432 |  3019.7576593972603  |
|  x.x.x.x | h100_sxm |     1     | 588ccb7b-4983-4515-a9e6-3e9e16d3e66d | 10f50209-e346-557c-87d1-ab32f7b21f0a | 5f01c537-e153-4475-8f85-830d2f32fdc4 |  2 days, 8:34:49.791775 |    0     |  38.256421684931524  |  1652.758843684933  |  3123.648290835619   |
|  x.x.x.x | h100_sxm |     1     | 5fa8ca19-653f-44db-9ea0-ac91205d38da | 2afe988d-be44-553f-9c85-3caa3d8c0f97 | d9c5cc54-d5b1-4a42-866d-59d291037d26 |  1 day, 9:24:42.791514  |    0     |  9.080310452054796   |  180.63118245205482 |  306.04619539726036  |
|  x.x.x.x | h100_sxm |     1     | 91887231-48ca-409d-9c38-13bcf65819be | 2afe988d-be44-553f-9c85-3caa3d8c0f97 | de3aa23c-81d6-4463-bcfa-e9e494afec91 |  1 day, 9:39:41.792103  |    0     |  7.553982698630138   |  156.8842678356165  |  262.8338168219179   |
|  x.x.x.x | h100_sxm |     1     | dac0f6fa-6d89-4d0c-b9c8-3331d88c598c | 10f50209-e346-557c-87d1-ab32f7b21f0a | 77ff2de3-d8ec-43c4-a85a-457c8cced256 |  2 days, 8:34:17.791723 |    0     |  35.58226475342467   |  1573.8952993150658 |  2950.052747671225   |
|  x.x.x.x | h100_sxm |     1     | fcc27b84-9faa-4692-a40f-b77a64d5e053 | 2b3d74b5-fa25-551c-9ea0-240949530eb7 | 5dc6e545-14f3-4d24-913b-16078f700c8c |      3:15:57.792154     |    0     |  22.20122157534247   |  53.415467013698645 |  53.41546701369863   |
