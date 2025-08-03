

pub const FUEL_BASE_ASSET: [u8; 32] = [
    0xf8, 0xf8, 0xb6, 0x28, 0x3d, 0x7f, 0xa5, 0xb6,
    0x72, 0xb5, 0x30, 0xcb, 0xb8, 0x4f, 0xcc, 0xcb,
    0x4f, 0xf8, 0xdc, 0x40, 0xf8, 0x17, 0x6e, 0xf4,
    0x54, 0x4d, 0xdb, 0x1f, 0x19, 0x52, 0xad, 0x07
];


// Zap colors using 256-color ANSI codes
pub const PURPLE_CUSTOM: &str = "\x1b[38;5;134m";    // #9351cc - Medium Purple
pub const LIME_GREEN: &str = "\x1b[38;5;192m";       // #b5ff7c - Bright Lime Green
pub const BRIGHT_PINK: &str = "\x1b[38;5;212m";      // #fd66ed - Bright Pink/Magenta

// Zap colors RGB mode (24-bit true color)
// Note: This requires terminal support for true color
pub const PURPLE_CUSTOM_RGB: &str = "\x1b[38;2;147;81;204m";  // #9351cc
pub const LIME_GREEN_RGB: &str = "\x1b[38;2;181;255;124m";    // #b5ff7c
pub const BRIGHT_PINK_RGB: &str = "\x1b[38;2;253;102;237m";   // #fd66ed

// Background versions (256-color)
pub const BG_PURPLE_CUSTOM: &str = "\x1b[48;5;134m";
pub const BG_LIME_GREEN: &str = "\x1b[48;5;192m";
pub const BG_BRIGHT_PINK: &str = "\x1b[48;5;212m";

// Background versions (RGB)
pub const BG_PURPLE_CUSTOM_RGB: &str = "\x1b[48;2;147;81;204m";
pub const BG_LIME_GREEN_RGB: &str = "\x1b[48;2;181;255;124m";
pub const BG_BRIGHT_PINK_RGB: &str = "\x1b[48;2;253;102;237m";

// Original bright colors (90-97 range)
pub const YELLOW: &str = "\x1b[93m";      // Bright Yellow
pub const RED: &str = "\x1b[91m";         // Bright Red
pub const GREEN: &str = "\x1b[92m";       // Bright Green
pub const BLUE: &str = "\x1b[94m";        // Bright Blue
pub const MAGENTA: &str = "\x1b[95m";     // Bright Magenta
pub const CYAN: &str = "\x1b[96m";        // Bright Cyan

// Standard colors (30-37 range) - darker variants
pub const DARK_YELLOW: &str = "\x1b[33m"; // Standard Yellow
pub const DARK_RED: &str = "\x1b[31m";    // Standard Red
pub const DARK_GREEN: &str = "\x1b[32m";  // Standard Green
pub const DARK_BLUE: &str = "\x1b[34m";   // Standard Blue
pub const DARK_MAGENTA: &str = "\x1b[35m"; // Standard Magenta
pub const DARK_CYAN: &str = "\x1b[36m";   // Standard Cyan

// Additional colors
pub const BLACK: &str = "\x1b[30m";       // Black
pub const GRAY: &str = "\x1b[90m";        // Bright Black (Gray)
pub const WHITE: &str = "\x1b[37m";       // White
pub const BRIGHT_WHITE: &str = "\x1b[97m"; // Bright White

// Orange (using 256-color mode)
pub const ORANGE: &str = "\x1b[38;5;208m"; // Orange
pub const DARK_ORANGE: &str = "\x1b[38;5;202m"; // Dark Orange

// Pink variations (using 256-color mode)
pub const PINK: &str = "\x1b[38;5;213m";  // Light Pink
pub const HOT_PINK: &str = "\x1b[38;5;205m"; // Hot Pink

// Purple variations
pub const PURPLE: &str = "\x1b[38;5;141m"; // Light Purple
pub const DARK_PURPLE: &str = "\x1b[38;5;91m"; // Dark Purple

// Style modifiers (can be combined with colors)
pub const BOLD: &str = "\x1b[1m";         // Bold
pub const DIM: &str = "\x1b[2m";          // Dim/Faint
pub const ITALIC: &str = "\x1b[3m";       // Italic
pub const UNDERLINE: &str = "\x1b[4m";    // Underline
pub const BLINK: &str = "\x1b[5m";        // Blink
pub const REVERSE: &str = "\x1b[7m";      // Reverse (swap foreground/background)
pub const STRIKETHROUGH: &str = "\x1b[9m"; // Strikethrough

// Background colors (bright)
pub const BG_YELLOW: &str = "\x1b[103m";  // Bright Yellow Background
pub const BG_RED: &str = "\x1b[101m";     // Bright Red Background
pub const BG_GREEN: &str = "\x1b[102m";   // Bright Green Background
pub const BG_BLUE: &str = "\x1b[104m";    // Bright Blue Background
pub const BG_MAGENTA: &str = "\x1b[105m"; // Bright Magenta Background
pub const BG_CYAN: &str = "\x1b[106m";    // Bright Cyan Background

// Background colors (standard)
pub const BG_DARK_YELLOW: &str = "\x1b[43m"; // Yellow Background
pub const BG_DARK_RED: &str = "\x1b[41m";    // Red Background
pub const BG_DARK_GREEN: &str = "\x1b[42m";  // Green Background
pub const BG_DARK_BLUE: &str = "\x1b[44m";   // Blue Background

pub const RESET: &str = "\x1b[0m";        // Reset all styles

