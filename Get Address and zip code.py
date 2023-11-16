from geopy.geocoders import Nominatim

# Create the grolocator object with a user-agent
geolocator = Nominatim(user_agent="geoapiExercises")

# Get the city name from the user
place = input("Enter City Name: ")

# Geocode the Location
location = geolocator.geocode(place)

# Print the geolocation details
print(location)

