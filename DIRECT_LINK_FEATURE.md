# Direct Link Download Feature

Download ZIP files directly from any URL without needing a GitHub repository.

## How to Use

1. Open **Main Menu** > **GitHub**
2. Press **Y** button to open "Direct Link"
3. Enter the full URL to a ZIP file (e.g., `https://example.com/homebrew.zip`)
4. Confirm and wait for download + extraction

## Behavior

### URL Validation
- URL **must** end with `.zip` (case-insensitive)
- Invalid URLs will show an error message

### File Size Warning
- Files **under 20 MB**: Download starts immediately
- Files **over 20 MB**: Warning dialog appears with options:
  - **Cancel** - Abort the download
  - **Force** - Download anyway (use with caution on slower connections)

### After Download
1. ZIP extracts automatically to SD card root (`/`)
2. Prompt: "Delete ZIP file?"
   - **Keep** - Retains ZIP at `/switch/sphaira/cache/github/direct_link.zip`
   - **Delete** - Removes the temporary ZIP
3. Prompt: "Save URL to history?"
   - **Yes** - URL appears in GitHub menu for future downloads
   - **No** - One-time download only

## Saved URLs

Saved URLs appear in the GitHub menu list as:
```
[filename] By Direct
```

### Storage Location
```
/config/sphaira/github/direct_links.json
```

### JSON Format
```json
[
    {"direct_url": "https://example.com/app1.zip"},
    {"direct_url": "https://example.com/app2.zip"},
    {"direct_url": "https://github.com/user/repo/releases/download/v1.0/release.zip"}
]
```

### Manual Editing
You can manually add URLs by editing `direct_links.json`:

1. Create the file at `/config/sphaira/github/direct_links.json` if it doesn't exist
2. Add entries following the JSON format above
3. Restart sphaira or re-enter the GitHub menu

## Error Messages

| Error | Cause | Solution |
|-------|-------|----------|
| "URL must end with .zip" | URL doesn't have `.zip` extension | Use a direct link to a ZIP file |
| "Download failed!" | Network error or invalid URL | Check internet connection and URL |
| "File is X MB (limit: 20 MB)" | File exceeds soft limit | Press "Force" to download anyway |

## Examples

### Valid URLs
```
https://github.com/user/repo/releases/download/v1.0/app.zip
https://example.com/homebrew/my-app.zip
https://cdn.site.com/files/package.ZIP
```

### Invalid URLs
```
https://github.com/user/repo              (no .zip extension)
https://example.com/file.7z               (not a ZIP file)
https://example.com/download?file=app     (no .zip extension)
```

## Technical Details

- Temporary download location: `/switch/sphaira/cache/github/direct_link.zip`
- History file: `/config/sphaira/github/direct_links.json`
- Extraction target: SD card root (`/`)
- Size check: HTTP HEAD request for Content-Length header
