// Simple transform — maps each CSV line to a BQ row
// Dataflow template calls this function for every line in the file
function transform(line) {
    // Skip the header row
    if (line.startsWith("id,")) return null;

    var parts = line.split(",");
    if (parts.length !== 6) return null;

    var row = {
        id:         parseInt(parts[0].trim()),
        name:       parts[1].trim(),
        age:        parseInt(parts[2].trim()),
        city:       parts[3].trim(),
        department: parts[4].trim(),
        salary:     parseInt(parts[5].trim())
    };

    return JSON.stringify(row);
}