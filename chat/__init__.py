"""
Chat Server Module
- REST API for chat functionality
- Socket.io for real-time communication
- Session-based authentication
"""

from .server import create_chat_app, get_socketio

__all__ = ['create_chat_app', 'get_socketio']
