import base64
import os

try:
    with open('static/logo.png', 'rb') as f:
        b64 = base64.b64encode(f.read()).decode('utf-8')

    svg_content = f'''<svg width="256" height="256" viewBox="0 0 256 256" xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink">
    <defs>
        <clipPath id="circle">
            <circle cx="128" cy="128" r="128" />
        </clipPath>
    </defs>
    <image href="data:image/png;base64,{b64}" width="256" height="256" clip-path="url(#circle)" />
</svg>'''

    with open('static/favicon.svg', 'w') as f:
        f.write(svg_content)
    
    print("SUCCESS: static/favicon.svg created")
except Exception as e:
    print(f"ERROR: {e}")
