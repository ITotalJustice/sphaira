# sphaira

A homebrew menu for the switch.

It was built for my usage, as such, features that may seem out of place are included because i found them useful.

[See the gbatemp thread for more details / discussion](https://gbatemp.net/threads/sphaira-hbmenu-replacement.664523/).

## showcase

|                          |                          |
:-------------------------:|:-------------------------:
![Img](assets/screenshots/2024121522512100-879193CD6A8B96CD00931A628B1187CB.jpg) | ![Img](assets/screenshots/2024121522514300-879193CD6A8B96CD00931A628B1187CB.jpg)
![Img](assets/screenshots/2024121522513300-879193CD6A8B96CD00931A628B1187CB.jpg) | ![Img](assets/screenshots/2024121523084100-879193CD6A8B96CD00931A628B1187CB.jpg)
![Img](assets/screenshots/2024121522505300-879193CD6A8B96CD00931A628B1187CB.jpg) | ![Img](assets/screenshots/2024121522502300-879193CD6A8B96CD00931A628B1187CB.jpg)
![Img](assets/screenshots/2024121523033200-879193CD6A8B96CD00931A628B1187CB.jpg) | ![Img](assets/screenshots/2024121523070300-879193CD6A8B96CD00931A628B1187CB.jpg)

## bug reports

for any bug reports, please use the issues tab and explain in as much detail as possible!

please include:

- CFW type (i assume Atmosphere, but someone out there is still using Rajnx)
- CFW version
- FW version
- The bug itself and how to reproduce it

## file assoc

sphaira has file assoc support. lets say your app supports loading .png files, then you could write an assoc file, then when using the file browser, clicking on a .png file will launch your app along with the .png file as argv[1]. This was primarly added for rom loading support for emulators / frontends such as retroarch, melonds, mgba etc.

```ini
[config]
path=/switch/your_app.nro
supported_extensions=jpg|png|mp4|mp3
```

the `path` field is optional. if left out, it will use the name of the ini to find the nro. For example, if the ini is called mgba.ini, it will try to find the nro in /switch/mgba.nro and /switch/folder/mgba.nro.

see `assets/romfs/assoc/` for more examples of file assoc entries

## Credits

- borealis
- stb
- yyjson
- nx-hbmenu
- nx-hbloader
- deko3d-nanovg
- libpulsar
- minIni
- gbatemp
- hb-appstore
- everyone who has contributed to this project!
