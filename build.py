import zipapp

from tenso.version import __version__

target_name = "dist/tenso-" + str(__version__) + ".pyz"
print("Building: " + target_name)
zipapp.create_archive(target=target_name, source="tenso")
