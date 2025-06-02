import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext
import threading
import pandas as pd
import asyncio
import aiohttp
import json
import os
from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
from functools import partial
import sys
import chardet
import re
import queue  # For thread-safe queue

# ---------------------------- Configuration ---------------------------- #

# GPT-4 API Configuration
# INCUBATOR_ENDPOINT = "https://eyq-incubator.asiapac.fabric.ey.com/eyq/as/api"
# INCUBATOR_KEY = "ff3jonCx0QML5i6GwScrekzRr9AJmgBh"  # **Replace with your actual API key**
INCUBATOR_ENDPOINT = "https://eyq-incubator.asiapac.fabric.ey.com/eyq/as/api"
INCUBATOR_KEY = "lgNlhkVWp94vwwPIzBY1VmFK6baEc0cb"
MODEL = "gpt-4-turbo"  # Model to use
API_VERSION = "2023-05-15"

# Database Configuration
DATABASE_URL = 'postgresql://pocuser:pocuser_8877@quark.centralindia.cloudapp.azure.com:5432/ankush_poc_db'  # **Replace with your actual database URL**

# Caching Configuration
NORMALIZATION_CACHE_FILE = 'normalization_cache.json'
CATEGORIZATION_CACHE_FILE = 'categorization_cache.json'

# ---------------------------- Helper Functions ---------------------------- #

def delete_cache_files():
    """Delete cache files if they exist."""
    for cache_file in [NORMALIZATION_CACHE_FILE, CATEGORIZATION_CACHE_FILE]:
        if os.path.exists(cache_file):
            try:
                os.remove(cache_file)
                print(f"Deleted cache file: {cache_file}")
            except Exception as e:
                print(f"Error deleting cache file {cache_file}: {e}")

def load_cache(file_path):
    """Load cache from a JSON file."""
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading cache from {file_path}: {e}")
    return {}

def save_cache(file_path, cache_data):
    """Save cache to a JSON file."""
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(cache_data, f, indent=4)
        print(f"Cache saved to {file_path}")
    except Exception as e:
        print(f"Error saving cache to {file_path}: {e}")

def create_openai_prompt(normalized_name, parent_company):
    """Create the prompt for OpenAI API."""
    prompt = f"""
As an expert in software categorization, please assist with the following task.

Given the software '{normalized_name}' developed by '{parent_company}', perform the following steps:

1. Provide a Brief Description: Write a concise description of the software in approximately 20 words.

2. Categorize the Software: Assign the software to the most appropriate Category and Subcategory from the list provided below.

**Categories and Subcategories:**

1. AI Tools & Services
    - AI Writing Tools
    - AI Chatbots
    - AI Art Generators
    - AI Music Generators
    - AI Companions
    - AI Prompt Tools
    - AI Avatars Generators
    - Conversational AI Tools
    - AI Email Assistants
2. Audio & Music
    - Audio Players
    - Music Production Apps
    - Music Streaming Services
    - Audio Recorders
    - Internet Radio Services
    - Music Discovery Services
    - Audio Converters
    - Music Libraries
    - Music Downloaders
    - Audio Editors
    - Audio Transcription Tools
    - Podcast Players
    - Music Games
    - Podcast Managers
    - Digital Audio Workstations
    - Mp3 Tag Editors
    - SoundCloud Downloaders
    - CD Rippers
    - Volume Control Tools
    - Music Recognition Apps
    - DJ Software
    - Music collections
    - Audio Routers
    - Metronomes
    - Audio CD Burners
    - Karaoke Players
    - Sound Equalizers
    - Audio Codecs
    - Codec Packs
    - MIDI Players
3. Backup & Sync
    - Cloud Storage Services
    - File Sync Tools
    - File Upload Services
    - Encrypted Backup Tools
    - Folder Sync Tools
    - Calendar Sync Tools
    - Disk Cloning Tools
    - Disk Imaging Tools
    - System Restore Tools
    - Backup Clients
    - System Backup Tools
    - Driver Backup Tools
    - SMS/Text Message Backup Tools
    - Contact Backup Tools
    - iPhone Backup Tools
    - Android Backup Tools
    - Email Backup Tools
    - Photo Synchronization Tools
4. Bitcoin & Cryptocurrency
    - Cryptocurrency Exchanges
    - Crypto Portfolio Trackers
    - Crypto Wallets
    - Cryptocurrency Coins
    - Bitcoin Mining Tools
    - Cryptocurrency Statistics Services
    - Bitcoin Payment Services
    - Crypto Mining Tools
    - Mining Pools
    - Cryptocurrency News
5. Business & Commerce
    - Project Management Tools
    - CRM Systems
    - Email Marketing Services
    - SEO Tools
    - CMS Tools
    - Invoicing Software
    - Customer Support Tools
    - Customer Feedback Managers
    - Personal Finance Tools
    - Business Intelligence Tools
    - Accounting Software
    - Help Desk Platforms
    - Survey Creators
    - Stock Trading Apps
    - E-commerce Systems
    - Cloud Computing Services
    - Expense Tracking Apps
    - Advertising Services
    - Payment Processing Services
    - Diagram Editors
    - Online Banking Tools
    - Money Transfer Services
    - Live Support Chats
    - Employee Performance Management Tools
    - Ad Networks
    - Shopping Carts
    - Online Store Builders
    - Affiliate Marketing Tools
    - Payment Gatewaies
    - Freelance Marketplaces
    - Bookkeeping Tools
    - A/B Testing Tools
    - Logo Makers
    - Billing Software
    - Payroll Systems
    - B2B Apps and Services
    - Investment Managers
6. CD/DVD Tools
    - DVD Rippers
    - CD Rippers
    - Bootable USB Creators
    - DVD Burners
    - DVD Copying Utilities
    - CD Burners
    - ISO Creators
    - Audio Extraction Tools
    - CD Label Makers
    - DVD Converters
    - Audio CD Burners
    - Bluray Burners
    - ISO Mounting Tools
    - CD Catalogers
    - DVD Decrypters
7. Development
    - Web Development Tools
    - Website Builders
    - Code Editors
    - Web Design Tools
    - Game Development Tools
    - IDEs
    - SQL Tools
    - Software Frameworks
    - Database Management Tools
    - Programming Languages
    - MySQL Tools
    - Mobile Development Tools
    - Version Control Systems
    - Javascript Libraries
    - Code Learning Services
    - Issue Tracking Systems
    - Web Servers
    - Package Managers
    - Static Site Generators
    - Javascript Development Tools
    - Documentation Generators
    - CSS Frameworks
    - Diff Tools
    - NoSQL Tools
    - Error Loggers
    - Android Development Tools
    - Pastebin Services
    - Git Clients
    - SCRUM Tools
    - HTTP Clients
    - Statistical Analyzers
    - UML Modeling Tools
    - PHP Development Tools
    - Source Code Hosting Services
    - XML Editors
    - Subversion Tools
    - JavaScript Frameworks
    - Web Debuggers
    - AI Coding Assistant Apps
    - Decompilers
    - Unit Testing Tools
    - HTTP(S) Debuggers
    - Web App Development Tools
    - NET Framework Tools
    - Big-Data Analytics Tools
    - C++ Development Tools
8. Education & Reference
    - Online Education Services
    - Language Learning Tools
    - Translators
    - Calculators
    - Flashcard Learning Tools
    - Wikis
    - Dictionaries
    - Educational Games
    - Code Learning Services
    - Q&A Services
    - Circuit Simulators
    - Grammar Checkers
    - Plagiarism Checkers
    - LaTeX Editors
    - Encyclopedias
    - Typing Tutors
    - Star Maps
    - Piano Trainers
    - Web Archiving Services
    - Electronic Design Automation Tools
    - Math Solvers
    - Exam Simulators
    - Circuit Designers
9. File Management
    - File Managers
    - File Search Utilities
    - Duplicate File Finders
    - File Tagging Tools
    - File Renamers
    - File Archivers
    - File Comparison Tools
    - File Copy Utilities
    - Folder Comparison Tools
    - Merge File Tools
    - File Unlockers
10. File Sharing
    - Download Managers
    - Large File Transfer Services
    - FTP Clients
    - Torrent Clients
    - File Sending Tools
    - Torrent Search Engines
    - Secure File Sharing Tools
    - FTP Servers
    - Torrent Streaming Services
    - File Hosting Downloaders
    - Torrent Seedboxs
    - Torrent Trackers
11. Games
    - Puzzle Games
    - Role-playing Games
    - Adventure Games
    - Strategy Games
    - Arcade Games
    - Simulation Games
    - First-Person Shooters
    - Platform Games
    - Racing Games
    - Action Games
    - RTS Games
    - Sandbox Games
    - Educational Games
    - Card Games
    - Side Scrolling Games
    - Open World Games
    - MMORPG Games
    - Survival Games
    - Turn Based Strategy Games
    - Exploration Games
    - Action RPG Games
    - Word Games
    - Board Games
    - Physics Games
    - Music Games
    - Quiz Games
    - Match-3 Games
    - Horror Games
    - Crafting Games
    - City Building Games
    - Point and Click Games
    - Space Games
    - Roguelike Games
    - Math Games
    - Chess Games
    - Tower Defense Games
    - Online Gaming Websites
    - Running Games
    - Dungeon Crawlers
    - Location-based Games
    - Fantasy Games
    - Soccer Games
    - Fighting Games
    - Clicking Games
    - Historical Games
    - Sports Games
    - Flight Simulators
    - Social Gaming Tools
    - Absorb Games
    - Endless Runner Games
    - Third-Person Shooters
    - Battle Royale Games
    - Tycoon Games
    - MOBA Games
    - Mobile Games
    - Factory Building Games
12. Gaming Software
    - Game Emulators
    - Virtual Tabletops
    - Game Stores
    - Game Library Managers
    - Game Launchers
    - Game Streaming Tools
    - Game Cheating Tools
    - Gamepad Mapping Apps
    - Chess Databases
    - Gaming-focused Tools
    - Gaming News
    - Anti-Cheat Tools
    - Game Chats
13. Home & Family
    - Online Shops
    - Educational Games
    - Kids Games
    - Price Comparison Services
    - Parental Control Tools
    - Recipe Managers
    - Home Automation Tools
    - Price Trackers
    - Genealogy Tools
    - Family Location Trackers
    - Home Inventory Apps
    - Grocery Delivery Services
    - Family Calendars
14. Network & Admin
    - Network Monitors
    - Web Analytics Services
    - Remote Desktop Tools
    - Uptime Monitor Services
    - Virtualization Tools
    - Web Log Analyzers
    - Penetration Testing Tools
    - Identity Management Tools
    - Remote Support Tools
    - SSH Clients
    - Network Analyzers
    - Wi-Fi Hotspots
    - Network Tools
    - Log Analyzers
    - Mobile Device Management (MDM) Tools
    - Packet Sniffers
    - Log Management Tools
    - Wi-Fi Scanners
    - Dynamic DNS Services
    - Network Mapping Tools
    - Website Downloaders
    - NAS Systems
    - Network Inventories
    - Router Custom Firmware
    - Network Simulators
15. News & Books
    - News Readers
    - RSS Readers
    - Ebook Readers
    - Tech News Sites
    - Comic and Manga Readers
    - Ebook Libraries
    - Novel Authoring Tools
    - Read It Later Tools
    - Social News
    - Offline Reading Tools
    - Usenet News Clients
    - Bible Study Tools
    - Book Recommendation Tools
    - Speed Reading Tools
    - Book Managers
    - Finance News
    - Ebook Conversion Tools
    - Sport News
    - Ebook Editors
    - News Sharing Tools
    - Audiobook Stores
    - Audiobook Converters
    - Convert PDF to Flipbooks
16. Office & Productivity
    - Note-taking Tools
    - Task Management Tools
    - Todo List Managers
    - Team Collaboration Tools
    - Time Tracking Tools
    - Calendar Apps
    - Text Editors
    - Kanban Boards
    - Document Managers
    - OCR Tools
    - Video Conferencing Tools
    - PDF Readers
    - PDF Editors
    - Email Clients
    - Mind Mapping Tools
    - Contact Managers
    - Spreadsheet Apps
    - Pomodoro Timers
    - Word Processors
    - Task Automation Apps
    - WebMail Providers
    - Clipboard Managers
    - Document Scanners
    - Presentation Makers
    - Poll Makers
    - Appointment Schedulers
    - Whiteboards
    - Personal Information Manager (PIM)s
    - Flow Charts
    - Text Expanders
    - Calendar Sync Tools
    - Anti Procrastination Tools
    - Office Suites
    - Sticky Notes Apps
    - Email Organizers
    - Document Readers
    - Desktop Publishers
17. Online Services
    - Online Shops
    - Web Hosting Services
    - Blog Publishing Tools
    - Job Search Services
    - Weather Forecast Tools
    - Cloud Hosting Services
    - URL Shorteners
    - VPS Hosting Providers
    - Classified Ad Services
    - WebMail Providers
    - App Stores
    - App Discovery Services
    - Web Search Engines
    - Mail Servers
    - Image Search Engines
    - Pastebin Services
    - Personal Homepage Sites
    - Reverse Image Search Engines
    - Similar Search Engines
    - People Search Engines
    - Grocery Delivery Services
    - Login Sharing Services
18. OS & Utilities
    - Operating Systems
    - File Recovery Tools
    - System Cleaners
    - Linux Distros
    - Application Launchers
    - Window Managers
    - Hardware Monitoring Tools
    - Terminal Emulators
    - Duplicate File Finders
    - System Information Utilities
    - Software Installers
    - Software Uninstallers
    - Benchmark Tools
    - Hard Disk Recovery Tools
    - Driver Updaters
    - System Tweakers
    - Software Updaters
    - Mobile Keyboards
    - Key Mapping Tools
    - Font Library Tools
    - Defrag Tools
    - Accessibility Tools
    - Android ROMs
    - Shell Extensions
    - Prevent Sleep Mode Apps
    - Partition Managers
    - Installer Creation Utilities
    - Compatibility Layer Tools
    - Process Monitoring Tools
    - Desktop Environments
    - Color Temperature Tools
    - Font Hosting Services
    - Boot Managers
    - Font Editors
    - Desktop Customization Tools
    - Hard Disk Diagnostic Tools
    - Android Emulators
    - Mobile Remote Control Tools
    - Driver Backup Tools
    - Keyboard Sharing Utilities
    - Mouse Sharing Utilities
    - Screen Magnifiers
    - Background Noise Reduction Tools
    - iOS Jailbreaking Apps
19. Photos & Graphics
    - Image Editors
    - Screenshot Capture Tools
    - Digital Painting Tools
    - 3D Modelers
    - Photo Sharing Apps
    - Photo Editors
    - Wallpapers Hubs
    - CAD Software
    - Vector Graphic Apps
    - Image Viewers
    - Graphic Design Tools
    - Resize Images Tools
    - Image Converters
    - Photo Managers
    - AI Art Generators
    - Image Optimizers
    - SVG Tools
    - Camera Apps
    - Animation Makers
    - Image Hosting Services
    - Photo Albums
    - Stock Photo Services
    - Duplicate Images Finders
    - Slideshow Makers
    - Image Upscaling Apps
    - Raw Photo Editors
    - Image Downloaders
    - Imageboards
    - Convert Videos to Animated GIFs
    - 3D Animators
    - Exif Renamers
    - Art / Design Communities
    - Icon Libraries
    - 3D Painting Apps
    - Face Filter Apps
    - Photo Libraries
    - Image Scanners
    - Remote Camera Control Tools
    - Photo Synchronization Tools
20. Remote Work & Education
    - Team Collaboration Tools
    - Online Education Services
    - Screen Sharing Tools
    - Remote Desktop Tools
    - Video Conferencing Tools
    - Live Streaming Tools and Services
    - Learning Management Systems
    - Employee Performance Management Tools
    - Online Courses
    - Interactive Whiteboards
    - Shared Calendars
    - Online Presentations
    - Classroom Management Tools
21. Security & Privacy
    - End-to-End Encryptions
    - VPN Services
    - File Recovery Tools
    - Privacy Protection Apps
    - Password Managers
    - Anti-Virus Apps
    - Anti-Malware Apps
    - Firewalls
    - Password Generators
    - Disposable Email Services
    - Parental Control Tools
    - File Encryption Tools
    - VPN Clients
    - Photo Recovery Tools
    - Online Anonymity Apps
    - Phone Trackers
    - Anonymous Proxy Apps
    - Vulnerability Scanners
    - Encrypted Email Services
    - Authenticators
    - Bypass Censorship Tools
    - File Shredders
    - Internet Filters
    - Password Recovery Apps
    - Disk Encryption Tools
    - Proxy Services
    - Disaster Recovery Tools
    - Device Tracker Apps
    - Data Breach Tools
    - Reverse Proxy Servers
22. Social & Communications
    - Social Networks
    - Group Chat Apps
    - Instant Messengers
    - Video Calling Apps
    - Photo Sharing Apps
    - Blog Publishing Tools
    - Voice Chats
    - Social Media Management Tools
    - Dating Services
    - Encrypted Chat Apps
    - Social Media Analytics
    - Chat Clients
    - Microblogs
    - Forums
    - Chat Rooms
    - Watch Videos Together Tools
    - Random Chats
    - Social Bookmarking Tools
    - Random Video Chat Apps
    - Mastodon Clients
    - IRC Clients
    - Smartphone Messaging Apps
    - Comment Platforms
    - Social Gaming Tools
    - Reddit Clients
    - AI Companions
    - Virtual Worlds
    - Art / Design Communities
    - Discord Mods
    - Twitter Clients
    - Nearby Chats
    - Instagram Clients
    - Game Chats
    - Chat without Internet Tools
    - Facebook Clients
23. Sport & Health
    - Health Tools
    - Habit Trackers
    - Meditation Tools
    - Fitness Trackers
    - Workouts Apps
    - Relaxation Tools
    - Calorie Trackers
    - Nutrition Trackers
    - Sport Video Analyzers
    - Weight Tracking Tools
    - Sports Trackers
    - Run Trackers
    - Weight Loss Tools
    - DICOM Viewers
    - Life Logging & Quantified Self Tools
    - Golf Apps
    - COVID-19 Tools
24. System & Hardware
    - Hardware Monitoring Tools
    - Disk Usage Analyzers
    - Server Management Tools
    - 3D Printing Tools
    - Driver Updaters
    - Key Mapping Tools
    - Screen Mirroring Apps
    - Battery Saver Utilities
    - Battery Monitors
    - Display Management Tools
    - Hard Disk Utilities
    - Temperature Monitoring Tools
    - Overclock Tools
    - Printing Tools
25. Travel & Location
    - Map Services
    - Travel Planners
    - GPS Navigation Services
    - Travel Guides
    - GIS Software
    - Flight Booking Services
    - Apps with Offline Map Support
    - Hotel Booking Systems
    - Accommodation Rental Services
    - Car Rental Services
    - Hotel Comparison Services
    - Flight Trackers
    - Travel Itineraries
    - COVID-19 Tools
26. Video & Movies
    - Video Editors
    - Video Streaming Apps
    - YouTube Downloaders
    - Video Converters
    - Video Downloaders
    - Screen Recorders
    - Media Players
    - Video Sharing Tools
    - Movie Streaming Services
    - TV Streaming Services
    - Media Managers
    - Movie Databases
    - Movie Review Sites
    - Chromecast Support Apps
    - Screen Casting Tools
    - Episode Trackers
    - Watch Videos Together Tools
    - Media Servers
    - YouTube Clients
    - Video Hosting Services
    - Movie Discovery Tools
    - Media Centers
    - Subtitle Editors
    - Subtitle Downloaders
    - Theater Show Control Apps
    - Projection Mapping Apps
    - Camera as Webcam Apps
    - Episode Downloaders
    - Codec Packs
    - Video Repair Utilities
27. Web Browsers
    - Ad Blockers
    - Bookmark Managers
    - Tab Managers
    - Chromium-based Browsers
    - Mobile Web Browsers
    - Firefox-based Browsers
    - Website Reloaders
    - Paywall Removers


**Format your response exactly as follows (do not include any extra text or greetings):**

1. Description:
2. Category:
3. Subcategory:

Please ensure that the Category and Subcategory you choose are exactly as listed above. If you cannot find an exact match, select the closest possible option.

"""
    return prompt

async def get_software_info_async(session, software_name, parent_company, mode='categorize', retry_count=0, max_retries=3):
    """Fetch software information from OpenAI API with retries for N/A responses."""
    if mode == 'normalize':
        prompt = f"""
Please provide only the normalized name for the software '{software_name}' by removing version numbers, editions, or any extra descriptors (e.g., 'Pro', '2016', 'v2.0'). The normalized name should represent the core product.

Format your response exactly as follows (do not include any extra text or greetings):

Normalized Software Name:

"""
    elif mode == 'categorize':
        prompt = create_openai_prompt(software_name, parent_company)

    body = {
        "messages": [
            {"role": "user", "content": prompt}
        ]
    }

    headers = {"api-key": INCUBATOR_KEY}
    query_params = {"api-version": API_VERSION}

    full_path = f"{INCUBATOR_ENDPOINT}/openai/deployments/{MODEL}/chat/completions"
    backoff_factor = 2

    try:
        async with session.post(full_path, json=body, headers=headers, params=query_params) as response:
            if response.status == 200:
                result = await response.json()
                content = result["choices"][0]["message"]["content"]
                return content
            else:
                error_text = await response.text()
                print(f"[ERROR] OpenAI request failed. Status Code: {response.status}, Response: {error_text}")
                return None
    except asyncio.CancelledError:
        return None
    except Exception as e:
        print(f"[ERROR] Exception occurred while querying OpenAI: {e}")
        if retry_count < max_retries:
            await asyncio.sleep(backoff_factor ** retry_count)
            return await get_software_info_async(session, software_name, parent_company, mode, retry_count + 1, max_retries)
        else:
            return None

# ---------------------------- Database Functions ---------------------------- #

def check_normalization_db(software_name):
    """Check if software_name exists in normalization DB."""
    with ENGINE.connect() as conn:
        query = text(
            "SELECT normalized_software_name FROM public.normalized_software_name WHERE software_name = :software_name"
        )
        result = conn.execute(query, {'software_name': software_name}).fetchone()
        if result:
            return result[0]
        return None

def save_normalization_db(software_name, normalized_name):
    """Save normalized software name to DB."""
    with ENGINE.connect() as conn:
        with conn.begin():
            insert_query = text(
                "INSERT INTO public.normalized_software_name (software_name, normalized_software_name) VALUES (:software_name, :normalized_software_name)"
            )
            conn.execute(insert_query, {'software_name': software_name, 'normalized_software_name': normalized_name})

def check_categorization_db(normalized_name, parent_company):
    """Check if (normalized_name, parent_company) exists in categorization DB."""
    with ENGINE.connect() as conn:
        query = text(
            "SELECT description, category, subcategory FROM master_software_info "
            "WHERE software_name = :software_name AND parent_company = :parent_company"
        )
        result = conn.execute(query, {
            'software_name': normalized_name,
            'parent_company': parent_company
        }).fetchone()
        if result:
            return {
                'software_name': normalized_name,
                'parent_company': parent_company,
                'description': result[0],
                'category': result[1],
                'subcategory': result[2],
                'status': 'Already Present in DB'
            }
        return None

def save_categorization_db(normalized_name, parent_company, description, category, subcategory):
    """Save categorized software info to DB."""
    with ENGINE.connect() as conn:
        with conn.begin():
            insert_query = text("""
                INSERT INTO master_software_info 
                (software_name, parent_company, description, category, subcategory)
                VALUES (:software_name, :parent_company, :description, :category, :subcategory)
            """)
            conn.execute(insert_query, {
                'software_name': normalized_name,
                'parent_company': parent_company,
                'description': description,
                'category': category,
                'subcategory': subcategory
            })

# ---------------------------- SQLAlchemy Engine Definition ---------------------------- #

# Create SQLAlchemy Engine
ENGINE = create_engine(DATABASE_URL, poolclass=QueuePool, pool_size=10)

# ---------------------------- GUI Application ---------------------------- #

class SoftwareCategorizationApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Software Categorization Tool")
        self.root.geometry("1600x900")  # Adjust as needed
        self.root.configure(bg='grey')

        # Initialize variables
        self.file_path = tk.StringVar()
        self.stop_event = threading.Event()
        self.processing_thread = None
        self.tasks = []  # Store tasks for cancellation

        # Mapping from index to Treeview row ID
        self.treeview_indices = {}

        # Counters for logging
        self.total_processed = 0
        self.total_normalized = 0
        self.total_categorized = 0
        self.total_read_from_cache = 0
        self.total_saved_to_db = 0
        self.category_counts = {}

        # Output DataFrame
        self.output_df = None  # Will store the output data
        self.df = None  # Store the processed DataFrame

        # Thread-safe queue for GUI updates
        self.update_queue = queue.Queue()

        # Delete cache files at start
        delete_cache_files()

        # Load caches
        self.normalization_cache = load_cache(NORMALIZATION_CACHE_FILE)
        self.categorization_cache = load_cache(CATEGORIZATION_CACHE_FILE)

        # Setup GUI
        self.setup_gui()

        # Start the periodic GUI update checker
        self.root.after(100, self.process_queue)

    def normalize_single_entry(self):
        software_name = self.software_name_var.get()
        if not software_name:
            messagebox.showerror("Error", "Please enter a software name.")
            return
        # Start a new thread to normalize the software name
        threading.Thread(target=self.normalize_single_software, args=(software_name,), daemon=True).start()

    def categorize_single_entry(self):
        software_name = self.software_name_var.get()
        parent_company = self.parent_company_var.get()
        if not software_name or not parent_company:
            messagebox.showerror("Error", "Please enter both software name and parent company.")
            return
        # Start a new thread to categorize the software
        threading.Thread(
            target=self.categorize_single_software, args=(software_name, parent_company), daemon=True
        ).start()

    def normalize_single_software(self, software_name):
        # Asynchronous normalization
        asyncio.run(self.async_normalize_single_software(software_name))

    async def async_normalize_single_software(self, software_name):
        try:
            async with aiohttp.ClientSession() as session:
                software_name, normalized_name, norm_status = await self.normalize_entry(session, software_name)
                output = (
                    f"Original Software Name: {software_name}\n"
                    f"Normalized Software Name: {normalized_name}\n"
                    f"Status: {norm_status}"
                )
                self.update_queue.put(('single_output', output))
        except Exception as e:
            self.log_message(f"Error normalizing software: {e}")
            self.update_queue.put(('single_output', f"Error: {e}"))

    def categorize_single_software(self, software_name, parent_company):
        # Asynchronous categorization
        asyncio.run(self.async_categorize_single_software(software_name, parent_company))

    async def async_categorize_single_software(self, software_name, parent_company):
        try:
            async with aiohttp.ClientSession() as session:
                # First, normalize the software name
                software_name, normalized_name, norm_status = await self.normalize_entry(session, software_name)
                # Then, categorize
                normalized_name, parent_company, description, category, subcategory, cat_status = await self.categorize_entry(
                    session, normalized_name, parent_company
                )
                output = (
                    f"Software Name: {normalized_name}\n"
                    f"Parent Company: {parent_company}\n"
                    f"Description: {description}\n"
                    f"Category: {category}\n"
                    f"Subcategory: {subcategory}\n"
                    f"Status: {cat_status}"
                )
                self.update_queue.put(('single_output', output))
        except Exception as e:
            self.log_message(f"Error categorizing software: {e}")
            self.update_queue.put(('single_output', f"Error: {e}"))

    def setup_gui(self):
        """Set up the GUI components."""
        # Style Configuration
        style = ttk.Style()
        style.theme_use('clam')  # Ensure the theme supports customization

        style.configure("Yellow.TButton", foreground="black", background="yellow")
        style.map("Yellow.TButton",
                  foreground=[('active', 'yellow')],
                  background=[('active', 'black')])

        style.configure('Treeview', background='grey', foreground='yellow', fieldbackground='grey',
                        font=('Helvetica', 10))
        style.configure('Treeview.Heading', background='black', foreground='yellow', font=('Helvetica', 12, 'bold'))

        style.configure("Yellow.Horizontal.TProgressbar", troughcolor='black', background='yellow')

        # Frame for Guidance Text
        guidance_frame = ttk.Frame(self.root, padding=10)
        guidance_frame.pack(fill='x')

        # Brief Guidance Label
        guidance_label = ttk.Label(
            guidance_frame,
            text="Welcome to the Software Categorization Tool. Select a CSV file and start processing.",
            foreground='yellow',
            background='grey',
            font=('Helvetica', 12)
        )
        guidance_label.pack(side='left', padx=(0, 10))

        # Help Button
        help_btn = ttk.Button(guidance_frame, text="Help", command=self.show_help, style='Yellow.TButton')
        help_btn.pack(side='right')

        # Frame for File Selection
        file_frame = ttk.Frame(self.root, padding=20)
        file_frame.pack(fill='x')

        select_btn = ttk.Button(file_frame, text="Select Input CSV File", command=self.select_file,
                                style='Yellow.TButton')
        select_btn.pack(side='left', padx=(0, 10))

        file_label = ttk.Label(file_frame, textvariable=self.file_path, foreground='yellow', background='grey')
        file_label.pack(side='left')

        # Frame for Control Buttons
        control_frame = ttk.Frame(self.root, padding=20)
        control_frame.pack(fill='x')

        start_btn = ttk.Button(control_frame, text="Start Processing", command=self.start_processing,
                               style='Yellow.TButton')
        start_btn.pack(side='left', padx=(0, 10))

        stop_btn = ttk.Button(control_frame, text="Stop Processing", command=self.stop_processing,
                              style='Yellow.TButton')
        stop_btn.pack(side='left', padx=(0, 10))

        self.download_btn = ttk.Button(control_frame, text="Download Output", command=self.download_output,
                                       style='Yellow.TButton', state='disabled')
        self.download_btn.pack(side='left', padx=(0, 10))

        exit_btn = ttk.Button(control_frame, text="Exit", command=self.exit_app, style='Yellow.TButton')
        exit_btn.pack(side='left', padx=(0, 10))

        # Progress Bar
        progress_frame = ttk.Frame(self.root, padding=20)
        progress_frame.pack(fill='x')

        self.progress_var = tk.DoubleVar()
        progress_bar = ttk.Progressbar(progress_frame, variable=self.progress_var, maximum=100,
                                       style="Yellow.Horizontal.TProgressbar")
        progress_bar.pack(fill='x')

        # Frame for Single Software Input
        single_entry_frame = ttk.Frame(self.root, padding=20)
        single_entry_frame.pack(fill='x')

        # Software Name Entry
        software_name_label = ttk.Label(
            single_entry_frame, text="Software Name:", foreground='yellow', background='grey'
        )
        software_name_label.pack(side='left', padx=(0, 10))
        self.software_name_var = tk.StringVar()
        software_name_entry = ttk.Entry(single_entry_frame, textvariable=self.software_name_var)
        software_name_entry.pack(side='left', padx=(0, 10))

        # Parent Company Entry
        parent_company_label = ttk.Label(
            single_entry_frame, text="Parent Company:", foreground='yellow', background='grey'
        )
        parent_company_label.pack(side='left', padx=(0, 10))
        self.parent_company_var = tk.StringVar()
        parent_company_entry = ttk.Entry(single_entry_frame, textvariable=self.parent_company_var)
        parent_company_entry.pack(side='left', padx=(0, 10))

        # Normalize Button
        normalize_btn = ttk.Button(
            single_entry_frame, text="Normalize", command=self.normalize_single_entry, style='Yellow.TButton'
        )
        normalize_btn.pack(side='left', padx=(0, 10))

        # Categorize Button
        categorize_btn = ttk.Button(
            single_entry_frame, text="Categorize", command=self.categorize_single_entry, style='Yellow.TButton'
        )
        categorize_btn.pack(side='left', padx=(0, 10))

        # Output Display
        self.single_output_text = scrolledtext.ScrolledText(
            single_entry_frame, height=5, bg='black', fg='yellow'
        )
        self.single_output_text.pack(fill='x', padx=(0, 10), pady=(10, 0))

        # Results Table
        table_frame = ttk.Frame(self.root, padding=20)
        table_frame.pack(fill='both', expand=True)

        columns = ("Software Name Original", "Parent Company", "Normalized Software Name", "Description", "Category",
                   "Subcategory", "Status")
        self.tree = ttk.Treeview(table_frame, columns=columns, show='headings')

        for col in columns:
            if col == "Description":
                self.tree.heading(col, text=col)
                self.tree.column(col, anchor='center', width=300)
            elif col == "Software Name Original" or col == "Parent Company":
                self.tree.heading(col, text=col)
                self.tree.column(col, anchor='center', width=200)
            else:
                self.tree.heading(col, text=col)
                self.tree.column(col, anchor='center', width=150)

        self.tree.pack(fill='both', expand=True, side='left')

        # Scrollbar for the table
        scrollbar = ttk.Scrollbar(table_frame, orient='vertical', command=self.tree.yview)
        self.tree.configure(yscroll=scrollbar.set)
        scrollbar.pack(side='right', fill='y')

        # Log Area
        log_frame = ttk.Frame(self.root, padding=20)
        log_frame.pack(fill='both', expand=False)

        log_label = ttk.Label(log_frame, text="Log:", foreground='yellow', background='grey')
        log_label.pack(anchor='w')

        self.log_text = scrolledtext.ScrolledText(log_frame, height=15, state='disabled', bg='black', fg='yellow')
        self.log_text.pack(fill='both', expand=True)

    def show_help(self):
        """Display detailed instructions on how to use the tool."""
        help_text = (
            "Instructions:\n\n"
            "1. **Select Input CSV File**: Click the 'Select Input CSV File' button to choose your input file. "
            "Ensure the CSV file contains 'software_name' and 'parent_company' columns.\n\n"
            "2. **Start Processing**: Click 'Start Processing' to begin normalization and categorization.\n\n"
            "3. **Monitor Progress**: Observe the progress bar, logs, and table updates.\n\n"
            "4. **Stop Processing**: If needed, click 'Stop Processing' to halt the operation immediately.\n\n"
            "5. **Download Output**: After processing is complete or stopped, click 'Download Output' to save the results.\n\n"
            "6. **Exit**: Click 'Exit' to close the application.\n\n"
            "Created by Kumar Ankush (kumar.ankush@in.ey.com)."
        )
        messagebox.showinfo("Help - Software Categorization Tool", help_text)

    def log_message(self, message):
        """Log messages to the log_text widget and print to console."""
        self.update_queue.put(('log', message))

    def select_file(self):
        """Handle file selection."""
        file_path = filedialog.askopenfilename(filetypes=[("CSV files", "*.csv")])
        if file_path:
            self.file_path.set(file_path)
            self.log_message(f"Selected file: {file_path}")
            self.populate_treeview(file_path)
        else:
            self.log_message("File selection canceled.")

    def populate_treeview(self, file_path):
        """Populate the Treeview with original software names and parent companies."""
        try:
            encoding, confidence = self.detect_file_encoding(file_path)
            self.log_message(f"Detected file encoding: {encoding} (Confidence: {confidence:.2f}%)")
            if confidence < 80:
                self.log_message("Low confidence in encoding detection. Proceeding with fallback encodings.")

            df = pd.read_csv(file_path, encoding=encoding)
            self.log_message("CSV file loaded successfully.")
        except UnicodeDecodeError as e:
            self.log_message(f"UnicodeDecodeError: {e}")
            # Attempt to read with fallback encodings
            fallback_encodings = ['utf-8', 'utf-16', 'iso-8859-1', 'windows-1252']
            for enc in fallback_encodings:
                try:
                    df = pd.read_csv(file_path, encoding=enc)
                    self.log_message(f"Successfully read CSV with fallback encoding: {enc}")
                    break
                except Exception as ex:
                    self.log_message(f"Failed to read CSV with encoding '{enc}': {ex}")
            else:
                self.log_message("All encoding attempts failed. Please check the file encoding and try again.")
                return
        except Exception as e:
            self.log_message(f"Error reading CSV file: {e}")
            return

        # Ensure required columns exist
        if not {'software_name', 'parent_company'}.issubset(df.columns):
            self.log_message("CSV file must contain 'software_name' and 'parent_company' columns.")
            return

        # Add 'normalized_name' column to df
        df['normalized_name'] = ''
        self.df_original = df.copy()  # Store the original DataFrame with 'normalized_name' column

        # Clear existing Treeview entries
        for item in self.tree.get_children():
            self.tree.delete(item)

        # Insert all rows into Treeview and keep track of indices
        self.treeview_indices = {}
        for index, row in df.iterrows():
            software_name = row['software_name']
            parent_company = row['parent_company']
            # Insert row with initial values
            row_id = self.tree.insert('', 'end', values=(
                software_name,
                parent_company,
                '',  # Normalized Software Name
                '',  # Description
                '',  # Category
                '',  # Subcategory
                'Pending'  # Status
            ))
            self.treeview_indices[index] = row_id

    def detect_file_encoding(self, file_path):
        """Detect the encoding of a file using chardet."""
        with open(file_path, 'rb') as f:
            raw_data = f.read(10000)  # Read first 10KB for detection
        result = chardet.detect(raw_data)
        encoding = result['encoding']
        confidence = result['confidence'] * 100  # Convert to percentage
        return encoding, confidence

    def start_processing(self):
        """Start the processing thread."""
        if not self.file_path.get():
            messagebox.showerror("Error", "Please select an input CSV file.")
            return

        if self.processing_thread and self.processing_thread.is_alive():
            messagebox.showwarning("Warning", "Processing is already running.")
            return

        self.stop_event.clear()
        self.tasks = []  # Reset the tasks list
        self.processing_thread = threading.Thread(target=self.process_file, daemon=True)
        self.processing_thread.start()
        self.log_message("Started processing.")

    def stop_processing(self):
        """Stop the processing thread."""
        if self.processing_thread and self.processing_thread.is_alive():
            self.stop_event.set()
            self.log_message("Stop requested. Stopping tasks...")
            # Cancel all running tasks
            for task in self.tasks:
                task.cancel()
            self.log_message("All tasks have been cancelled.")
        else:
            self.log_message("No active processing to stop.")

    def exit_app(self):
        """Exit the application gracefully."""
        if self.processing_thread and self.processing_thread.is_alive():
            if messagebox.askyesno("Exit", "Processing is running. Do you want to exit?"):
                self.stop_event.set()
                self.root.quit()
        else:
            self.root.quit()

    def process_file(self):
        """Run the asynchronous processing."""
        asyncio.run(self.async_process())

    async def async_process(self):
        """Asynchronous processing of the CSV file."""
        try:
            file_path = self.file_path.get()
            encoding, confidence = self.detect_file_encoding(file_path)
            self.log_message(f"Detected file encoding: {encoding} (Confidence: {confidence:.2f}%)")
            if confidence < 80:
                self.log_message("Low confidence in encoding detection. Proceeding with fallback encodings.")

            # Load the CSV file
            df = await self.load_csv_async(file_path, encoding)

            # Ensure required columns exist
            if not {'software_name', 'parent_company'}.issubset(df.columns):
                self.log_message("CSV file must contain 'software_name' and 'parent_company' columns.")
                return

            # Add 'normalized_name' column
            df['normalized_name'] = ''

            # Assign self.df early to avoid AttributeError
            self.df = df

            # Deduplicate software names for normalization
            unique_software_names = df['software_name'].unique()
            self.log_message(f"Total unique software names to normalize: {len(unique_software_names)}")

            # Normalize software names concurrently
            self.log_message("Starting normalization...")
            normalization_results = {}
            software_name_to_index = {}  # Map software_name to list of indices
            for idx, name in df['software_name'].items():
                software_name_to_index.setdefault(name, []).append(idx)

            async with aiohttp.ClientSession() as session:
                normalization_tasks = [
                    asyncio.create_task(self.normalize_entry(session, software_name))
                    for software_name in unique_software_names
                ]
                self.tasks.extend(normalization_tasks)
                normalization_completed, _ = await asyncio.wait(normalization_tasks)
                for task in normalization_completed:
                    software_name, normalized_name, norm_status = await task
                    normalization_results[software_name] = (normalized_name, norm_status)
                    self.total_normalized += 1
                    if norm_status == 'Cached' or norm_status == 'Already Present in DB':
                        self.total_read_from_cache += 1
                    # Queue the GUI updates for all indices of this software_name
                    indices = software_name_to_index[software_name]
                    for index in indices:
                        self.update_queue.put(('normalization', index, normalized_name))
                    self.log_message(f"Normalized '{software_name}' to '{normalized_name}'. Status: {norm_status}")

            # Map normalized names back to the DataFrame
            df['normalized_name'] = df['software_name'].map(lambda x: normalization_results.get(x, (x, 'Error'))[0])

            # Deduplicate (normalized_name, parent_company) pairs for categorization
            unique_normalized_pairs = df[['normalized_name', 'parent_company']].drop_duplicates()
            self.log_message(f"Total unique normalized entries to categorize: {len(unique_normalized_pairs)}")

            # Map (normalized_name, parent_company) to indices
            pair_to_indices = {}
            for idx, row in df.iterrows():
                key = (row['normalized_name'], row['parent_company'])
                pair_to_indices.setdefault(key, []).append(idx)

            # Categorize software concurrently
            self.log_message("Starting categorization...")
            categorization_results = {}
            async with aiohttp.ClientSession() as session:
                categorization_tasks = []
                task_to_pair = {}
                for _, row in unique_normalized_pairs.iterrows():
                    normalized_name = row['normalized_name']
                    parent_company = row['parent_company']
                    task = asyncio.create_task(self.categorize_entry(session, normalized_name, parent_company))
                    categorization_tasks.append(task)
                    task_to_pair[task] = (normalized_name, parent_company)
                self.tasks.extend(categorization_tasks)
                total = len(categorization_tasks)
                idx = 0
                for task in asyncio.as_completed(categorization_tasks):
                    if self.stop_event.is_set():
                        self.log_message("Processing stopped by user.")
                        self.update_queue.put(('update_progress', 0))
                        break
                    try:
                        normalized_name, parent_company, description, category, subcategory, cat_status = await task
                        key = (normalized_name, parent_company)
                        categorization_results[key] = (description, category, subcategory, cat_status)
                        progress = (idx + 1) / total * 100
                        self.update_queue.put(('update_progress', progress))
                        self.log_message(f"Categorized '{normalized_name}' for '{parent_company}'. Status: {cat_status}")
                        self.total_categorized += 1
                        idx += 1

                        # Update category counts
                        if category in self.category_counts:
                            self.category_counts[category] += 1
                        else:
                            self.category_counts[category] = 1

                        # Update saved to DB count if status is 'Updated'
                        if cat_status == 'Updated':
                            self.total_saved_to_db += 1

                        # Queue the GUI updates for all indices of this pair
                        indices = pair_to_indices.get(key, [])
                        for index in indices:
                            self.update_queue.put(('categorization', index, description, category, subcategory, cat_status))

                    except asyncio.CancelledError:
                        self.log_message("Categorization task was cancelled.")
                        break
                    except Exception as e:
                        self.log_message(f"Error during categorization: {e}")

            # Map categorization results back to the DataFrame
            df['description'] = ''
            df['category'] = ''
            df['subcategory'] = ''
            df['status'] = ''
            for idx, row in df.iterrows():
                normalized_name = row['normalized_name']
                parent_company = row['parent_company']
                key = (normalized_name, parent_company)
                if key in categorization_results:
                    description, category, subcategory, status = categorization_results[key]
                    df.at[idx, 'description'] = description
                    df.at[idx, 'category'] = category
                    df.at[idx, 'subcategory'] = subcategory
                    df.at[idx, 'status'] = status

            # Save caches
            self.save_caches()

            # Prepare output DataFrame
            self.output_df = df[['software_name', 'parent_company', 'normalized_name', 'description', 'category', 'subcategory', 'status']]

            # Store the processed DataFrame
            self.df = df.copy()

            self.update_queue.put(('enable_download',))
            self.update_queue.put(('update_progress', 100))

            # Display summary
            summary = (
                f"Processing completed successfully.\n"
                f"Total Unique Software Names Normalized: {len(unique_software_names)}\n"
                f"Total Unique Entries Categorized: {len(unique_normalized_pairs)}\n"
                f"Total Normalized: {self.total_normalized}\n"
                f"Total Categorized: {self.total_categorized}\n"
                f"Total Read from Cache: {self.total_read_from_cache}\n"
                f"Total Saved to Database: {self.total_saved_to_db}\n"
                f"Software Count by Category:\n"
            )
            for category, count in self.category_counts.items():
                summary += f" - {category}: {count}\n"

            self.log_message(summary)

        except Exception as e:
            self.log_message(f"An error occurred during processing: {e}")
            self.update_queue.put(('enable_download',))

    async def load_csv_async(self, file_path, encoding):
        """Load the CSV file asynchronously."""
        loop = asyncio.get_event_loop()
        df = await loop.run_in_executor(None, partial(pd.read_csv, file_path, encoding=encoding))
        return df

    async def normalize_entry(self, session, software_name, retry_attempt=0, max_retries=3):
        """Normalize the software name with retries if necessary."""
        try:
            if software_name in self.normalization_cache:
                normalized_name = self.normalization_cache[software_name]
                status = 'Cached'
                return software_name, normalized_name, status

            # Check database asynchronously
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, check_normalization_db, software_name)

            if result:
                normalized_name = result
                status = 'Already Present in DB'
                self.normalization_cache[software_name] = normalized_name
                return software_name, normalized_name, status

            # Fetch normalized name via API
            normalized_name_response = await get_software_info_async(session, software_name, None, mode='normalize', retry_count=retry_attempt, max_retries=max_retries)
            if not normalized_name_response:
                if retry_attempt < max_retries:
                    self.log_message(f"Retrying normalization for '{software_name}' (Attempt {retry_attempt + 1})")
                    return await self.normalize_entry(session, software_name, retry_attempt + 1, max_retries)
                else:
                    normalized_name = software_name  # Fallback
                    status = 'Error'
            else:
                try:
                    # Use regex to extract the normalized software name
                    match = re.search(r"Normalized Software Name:\s*(.*)", normalized_name_response, re.IGNORECASE)
                    if match:
                        normalized_name = match.group(1).strip()
                        if not normalized_name:
                            raise ValueError("Empty normalized name.")
                        status = 'Updated'
                    else:
                        raise ValueError("Normalized Software Name not found in the response.")
                except Exception as e:
                    self.log_message(f"Parsing error during normalization for '{software_name}': {e}")
                    if retry_attempt < max_retries:
                        self.log_message(f"Retrying normalization for '{software_name}' (Attempt {retry_attempt + 1})")
                        return await self.normalize_entry(session, software_name, retry_attempt + 1, max_retries)
                    else:
                        normalized_name = software_name  # Fallback
                        status = 'Error'

            # Update cache
            self.normalization_cache[software_name] = normalized_name

            # Save to database asynchronously
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, save_normalization_db, software_name, normalized_name)

            return software_name, normalized_name, status
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.log_message(f"Error during normalization for '{software_name}': {e}")
            return software_name, software_name, 'Error'

    async def categorize_entry(self, session, normalized_name, parent_company, retry_attempt=0, max_retries=3):
        """Categorize the normalized software with retries if necessary."""
        try:
            key = f"{normalized_name}|{parent_company}"

            if key in self.categorization_cache:
                result = self.categorization_cache[key]
                result['status'] = 'Cached'
                self.total_read_from_cache += 1
                return normalized_name, parent_company, result['description'], result['category'], result['subcategory'], result['status']

            # Check database asynchronously
            loop = asyncio.get_event_loop()
            result_dict = await loop.run_in_executor(None, partial(check_categorization_db, normalized_name, parent_company))

            if result_dict:
                self.categorization_cache[key] = result_dict
                return normalized_name, parent_company, result_dict['description'], result_dict['category'], result_dict['subcategory'], result_dict['status']

            # Fetch categorization via API
            response = await get_software_info_async(session, normalized_name, parent_company, mode='categorize', retry_count=retry_attempt, max_retries=max_retries)
            if not response:
                if retry_attempt < max_retries:
                    self.log_message(f"Retrying categorization for '{normalized_name}' by '{parent_company}' (Attempt {retry_attempt + 1})")
                    return await self.categorize_entry(session, normalized_name, parent_company, retry_attempt + 1, max_retries)
                else:
                    description = category = subcategory = 'N/A'
                    status = 'Error'
            else:
                try:
                    # Use regex to extract the description, category, and subcategory
                    desc_match = re.search(r"1\. Description:\s*(.*)", response, re.IGNORECASE)
                    cat_match = re.search(r"2\. Category:\s*(.*)", response, re.IGNORECASE)
                    subcat_match = re.search(r"3\. Subcategory:\s*(.*)", response, re.IGNORECASE)

                    if desc_match and cat_match and subcat_match:
                        description = desc_match.group(1).strip()
                        category = cat_match.group(1).strip()
                        subcategory = subcat_match.group(1).strip()

                        if not all([description, category, subcategory]):
                            raise ValueError("Incomplete categorization data.")
                        status = 'Updated'
                    else:
                        raise ValueError("Required fields not found in the response.")
                except (IndexError, ValueError) as e:
                    self.log_message(f"Parsing error during categorization for '{normalized_name}': {e}")
                    if retry_attempt < max_retries:
                        self.log_message(f"Retrying categorization for '{normalized_name}' by '{parent_company}' (Attempt {retry_attempt + 1})")
                        return await self.categorize_entry(session, normalized_name, parent_company, retry_attempt + 1, max_retries)
                    else:
                        description = category = subcategory = 'N/A'
                        status = 'Error'

            result = {
                'software_name': normalized_name,
                'parent_company': parent_company,
                'description': description,
                'category': category,
                'subcategory': subcategory,
                'status': status
            }

            self.categorization_cache[key] = result

            # Save to database asynchronously
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, partial(
                save_categorization_db,
                normalized_name,
                parent_company,
                description,
                category,
                subcategory
            ))

            return normalized_name, parent_company, description, category, subcategory, status
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.log_message(f"Error during categorization for '{normalized_name}': {e}")
            return normalized_name, parent_company, 'N/A', 'N/A', 'N/A', 'Error'

    def update_treeview_row_normalization(self, index, normalized_name):
        """Update the Treeview row with the normalized name."""
        if index in self.treeview_indices:
            row_id = self.treeview_indices[index]
            current_values = self.tree.item(row_id)['values']
            new_values = list(current_values)
            new_values[2] = normalized_name  # Normalized Software Name
            self.tree.item(row_id, values=new_values)

    def update_treeview_row_categorization(self, index, description, category, subcategory, status):
        """Update the Treeview row with categorization results."""
        if index in self.treeview_indices:
            row_id = self.treeview_indices[index]
            current_values = self.tree.item(row_id)['values']
            new_values = list(current_values)
            new_values[3] = description
            new_values[4] = category
            new_values[5] = subcategory
            new_values[6] = status
            self.tree.item(row_id, values=new_values)

    def update_progress(self, value):
        """Update the progress bar."""
        self.progress_var.set(value)
        self.root.update_idletasks()

    def save_caches(self):
        """Save normalization and categorization caches to files."""
        save_cache(NORMALIZATION_CACHE_FILE, self.normalization_cache)
        save_cache(CATEGORIZATION_CACHE_FILE, self.categorization_cache)
        self.log_message("Caches saved successfully.")

    def enable_download_button(self):
        """Enable the Download Output button."""
        self.download_btn.config(state='normal')

    def download_output(self):
        """Handle downloading the output CSV file."""
        if self.output_df is not None:
            file_path = filedialog.asksaveasfilename(defaultextension='.csv', filetypes=[("CSV files", "*.csv")])
            if file_path:
                try:
                    self.output_df.to_csv(file_path, index=False, encoding='utf-8-sig')  # Use utf-8-sig for better compatibility
                    self.log_message(f"Output saved to '{file_path}'.")
                except Exception as e:
                    self.log_message(f"Error saving output CSV: {e}")
        else:
            messagebox.showwarning("Warning", "No output data to save.")

    def process_queue(self):
        """Process the update queue and apply GUI updates."""
        try:
            while True:
                update = self.update_queue.get_nowait()
                if update[0] == 'log':
                    message = update[1]
                    self.log_text.config(state='normal')
                    self.log_text.insert(tk.END, message + "\n")
                    self.log_text.see(tk.END)
                    self.log_text.config(state='disabled')
                elif update[0] == 'normalization':
                    index, normalized_name = update[1], update[2]
                    self.update_treeview_row_normalization(index, normalized_name)
                elif update[0] == 'categorization':
                    index, description, category, subcategory, status = (
                        update[1],
                        update[2],
                        update[3],
                        update[4],
                        update[5],
                    )
                    self.update_treeview_row_categorization(
                        index, description, category, subcategory, status
                    )
                elif update[0] == 'enable_download':
                    self.enable_download_button()
                elif update[0] == 'update_progress':
                    value = update[1]
                    self.update_progress(value)
                elif update[0] == 'single_output':
                    output = update[1]
                    self.single_output_text.config(state='normal')
                    self.single_output_text.delete('1.0', tk.END)
                    self.single_output_text.insert(tk.END, output)
                    self.single_output_text.config(state='disabled')
        except queue.Empty:
            pass
        # Schedule the next queue processing
        self.root.after(100, self.process_queue)


# ---------------------------- Main Execution ---------------------------- #

if __name__ == "__main__":
    root = tk.Tk()
    app = SoftwareCategorizationApp(root)
    root.mainloop()
